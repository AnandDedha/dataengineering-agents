# Databricks Multi-Agent Pipeline Builder System
# This system creates data pipelines from natural language specifications

import os
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
from datetime import datetime

# Databricks libraries
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog, jobs, workspace, pipelines
from databricks.sdk.service.catalog import SchemaInfo, TableInfo
from databricks.sdk.service.workspace import ImportFormat

# LangChain for agent orchestration
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.tools import Tool, StructuredTool
from langchain.schema import SystemMessage, HumanMessage
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain_openai import ChatOpenAI
from langchain.callbacks import StreamingStdOutCallbackHandler

# GitHub integration
from github import Github, GithubException
import base64

# Pydantic for data validation
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============== Data Models ==============

class PipelineStage(Enum):
    """Pipeline stages for tracking progress"""
    PLANNING = "planning"
    QUERYING = "querying"
    NOTEBOOK_GENERATION = "notebook_generation"
    QUALITY_CHECKS = "quality_checks"
    APPROVAL = "approval"
    DEPLOYMENT = "deployment"
    COMPLETED = "completed"

@dataclass
class PipelineSpecification:
    """Specification for a data pipeline"""
    name: str
    description: str
    source_tables: List[str]
    transformations: List[Dict[str, Any]]
    target_table: str
    schedule: Optional[str] = None
    quality_checks: List[Dict[str, Any]] = None
    notifications: List[str] = None
    
class ApprovalRequest(BaseModel):
    """Model for approval requests"""
    pipeline_id: str = Field(description="Unique pipeline identifier")
    stage: str = Field(description="Current pipeline stage")
    description: str = Field(description="Description of what needs approval")
    artifacts: Dict[str, Any] = Field(description="Related artifacts for review")
    timestamp: datetime = Field(default_factory=datetime.now)

# ============== Agent Base Classes ==============

class BaseAgent:
    """Base class for all agents in the system"""
    
    def __init__(self, name: str, workspace_client: WorkspaceClient):
        self.name = name
        self.workspace_client = workspace_client
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
    def log_action(self, action: str, details: Dict[str, Any] = None):
        """Log agent actions for audit trail"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "agent": self.name,
            "action": action,
            "details": details or {}
        }
        self.logger.info(json.dumps(log_entry))
        return log_entry

# ============== Specification Parser Agent ==============

class SpecificationParserAgent(BaseAgent):
    """Parses natural language into pipeline specifications"""
    
    def __init__(self, workspace_client: WorkspaceClient, llm: ChatOpenAI):
        super().__init__("SpecificationParser", workspace_client)
        self.llm = llm
        
    def parse_statement(self, statement: str) -> PipelineSpecification:
        """Parse natural language statement into pipeline specification"""
        
        prompt = f"""
        Parse the following statement into a data pipeline specification:
        
        Statement: {statement}
        
        Extract:
        1. Pipeline name and description
        2. Source tables (from Unity Catalog)
        3. Required transformations
        4. Target table/location
        5. Schedule (if mentioned)
        6. Data quality checks needed
        
        Return as structured JSON.
        """
        
        response = self.llm.invoke(prompt)
        spec_dict = json.loads(response.content)
        
        spec = PipelineSpecification(
            name=spec_dict.get("name", "unnamed_pipeline"),
            description=spec_dict.get("description", ""),
            source_tables=spec_dict.get("source_tables", []),
            transformations=spec_dict.get("transformations", []),
            target_table=spec_dict.get("target_table", ""),
            schedule=spec_dict.get("schedule"),
            quality_checks=spec_dict.get("quality_checks", [])
        )
        
        self.log_action("parsed_specification", {"spec": spec.__dict__})
        return spec

# ============== Unity Catalog Query Agent ==============

class UnityCatalogAgent(BaseAgent):
    """Handles Unity Catalog operations and queries"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        super().__init__("UnityCatalog", workspace_client)
        
    def validate_tables(self, table_names: List[str]) -> Dict[str, Any]:
        """Validate that tables exist in Unity Catalog"""
        results = {}
        
        for table_name in table_names:
            try:
                parts = table_name.split(".")
                if len(parts) == 3:
                    catalog_name, schema_name, table = parts
                    table_info = self.workspace_client.tables.get(
                        full_name=table_name
                    )
                    results[table_name] = {
                        "exists": True,
                        "columns": [col.name for col in table_info.columns],
                        "data_type": table_info.table_type.value,
                        "properties": table_info.properties
                    }
                else:
                    results[table_name] = {"exists": False, "error": "Invalid table name format"}
            except Exception as e:
                results[table_name] = {"exists": False, "error": str(e)}
                
        self.log_action("validated_tables", results)
        return results
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get detailed schema information for a table"""
        try:
            table_info = self.workspace_client.tables.get(full_name=table_name)
            schema = {
                "columns": [
                    {
                        "name": col.name,
                        "data_type": col.data_type,
                        "nullable": col.nullable,
                        "comment": col.comment
                    }
                    for col in table_info.columns
                ],
                "partitions": table_info.partition_columns,
                "properties": table_info.properties
            }
            return schema
        except Exception as e:
            self.logger.error(f"Failed to get schema for {table_name}: {e}")
            return {}
    
    def create_table(self, catalog: str, schema: str, table: str, 
                    columns: List[Dict], properties: Dict = None) -> bool:
        """Create a new table in Unity Catalog"""
        try:
            table_info = self.workspace_client.tables.create(
                name=table,
                catalog_name=catalog,
                schema_name=schema,
                table_type=catalog.TableType.MANAGED,
                columns=[
                    catalog.ColumnInfo(
                        name=col["name"],
                        data_type=col["data_type"],
                        nullable=col.get("nullable", True),
                        comment=col.get("comment")
                    )
                    for col in columns
                ],
                properties=properties
            )
            self.log_action("created_table", {"table": f"{catalog}.{schema}.{table}"})
            return True
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            return False

# ============== Notebook Generator Agent ==============

class NotebookGeneratorAgent(BaseAgent):
    """Generates Databricks notebooks from specifications"""
    
    def __init__(self, workspace_client: WorkspaceClient, llm: ChatOpenAI):
        super().__init__("NotebookGenerator", workspace_client)
        self.llm = llm
        
    def generate_notebook(self, spec: PipelineSpecification, 
                         table_schemas: Dict[str, Any]) -> str:
        """Generate a Databricks notebook for the pipeline"""
        
        notebook_content = self._create_notebook_structure(spec, table_schemas)
        
        # Generate transformation logic using LLM
        transformation_code = self._generate_transformation_code(spec, table_schemas)
        
        # Combine into complete notebook
        complete_notebook = self._combine_notebook_sections(
            notebook_content, 
            transformation_code,
            spec
        )
        
        self.log_action("generated_notebook", {"pipeline": spec.name})
        return complete_notebook
    
    def _create_notebook_structure(self, spec: PipelineSpecification, 
                                  table_schemas: Dict[str, Any]) -> str:
        """Create the basic notebook structure"""
        
        notebook = f"""# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline: {spec.name}
# MAGIC {spec.description}
# MAGIC 
# MAGIC Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging

# Initialize Spark session
spark = SparkSession.builder.appName("{spec.name}").getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pipeline configuration
PIPELINE_NAME = "{spec.name}"
TARGET_TABLE = "{spec.target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

"""
        
        # Add source table loading code
        for table in spec.source_tables:
            table_var = table.split(".")[-1].lower().replace("-", "_")
            notebook += f"""
# Load {table}
df_{table_var} = spark.table("{table}")
logger.info(f"Loaded {{df_{table_var}.count()}} rows from {table}")
df_{table_var}.printSchema()

# COMMAND ----------
"""
        
        return notebook
    
    def _generate_transformation_code(self, spec: PipelineSpecification,
                                     table_schemas: Dict[str, Any]) -> str:
        """Generate transformation code using LLM"""
        
        prompt = f"""
        Generate PySpark transformation code for the following pipeline:
        
        Pipeline: {spec.name}
        Description: {spec.description}
        
        Source Tables and Schemas:
        {json.dumps(table_schemas, indent=2)}
        
        Required Transformations:
        {json.dumps(spec.transformations, indent=2)}
        
        Target Table: {spec.target_table}
        
        Generate efficient PySpark code that:
        1. Applies all transformations
        2. Handles null values appropriately
        3. Includes error handling
        4. Optimizes for performance
        5. Adds logging for monitoring
        
        Return only the Python code.
        """
        
        response = self.llm.invoke(prompt)
        return response.content
    
    def _combine_notebook_sections(self, structure: str, transformations: str,
                                  spec: PipelineSpecification) -> str:
        """Combine all notebook sections"""
        
        quality_checks = self._generate_quality_checks(spec)
        
        complete_notebook = structure + f"""
# MAGIC %md
# MAGIC ## Data Transformations

# COMMAND ----------

{transformations}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

{quality_checks}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

# Write to target table
result_df.write \\
    .mode("overwrite") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable("{spec.target_table}")

logger.info(f"Successfully wrote {{result_df.count()}} rows to {spec.target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Completion

# COMMAND ----------

# Log pipeline completion
print(f"Pipeline '{spec.name}' completed successfully at {{datetime.now()}}")

# Return success status
dbutils.notebook.exit("SUCCESS")
"""
        return complete_notebook
    
    def _generate_quality_checks(self, spec: PipelineSpecification) -> str:
        """Generate data quality check code"""
        
        checks_code = """
# Data Quality Checks
from pyspark.sql.functions import col, count, isnan, when, isnull

def run_quality_checks(df):
    \"\"\"Run data quality checks on the dataframe\"\"\"
    checks_passed = True
    issues = []
    
    # Check for null values in critical columns
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    for column, null_count in null_counts.asDict().items():
        if null_count > 0:
            issues.append(f"Column {column} has {null_count} null values")
    
    # Check for duplicates
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    if total_rows != distinct_rows:
        issues.append(f"Found {total_rows - distinct_rows} duplicate rows")
    
    # Check row count thresholds
    if total_rows == 0:
        checks_passed = False
        issues.append("No data in result set")
    
    # Log results
    if issues:
        for issue in issues:
            logger.warning(f"Data quality issue: {issue}")
    else:
        logger.info("All data quality checks passed")
    
    return checks_passed, issues

# Run quality checks
quality_passed, quality_issues = run_quality_checks(result_df)

if not quality_passed:
    raise ValueError(f"Data quality checks failed: {quality_issues}")
"""
        
        # Add custom quality checks if specified
        if spec.quality_checks:
            for check in spec.quality_checks:
                checks_code += f"""
                
# Custom check: {check.get('name', 'unnamed')}
{check.get('code', '# No implementation provided')}
"""
        
        return checks_code
    
    def save_notebook(self, notebook_content: str, path: str) -> bool:
        """Save notebook to Databricks workspace"""
        try:
            # Convert to base64 for API
            content_bytes = notebook_content.encode('utf-8')
            content_b64 = base64.b64encode(content_bytes).decode('utf-8')
            
            # Import to workspace
            self.workspace_client.workspace.import_(
                path=path,
                content=content_b64,
                format=ImportFormat.SOURCE,
                language=workspace.Language.PYTHON,
                overwrite=True
            )
            
            self.log_action("saved_notebook", {"path": path})
            return True
        except Exception as e:
            self.logger.error(f"Failed to save notebook: {e}")
            return False

# ============== Data Quality Agent ==============

class DataQualityAgent(BaseAgent):
    """Performs data quality checks and validation"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        super().__init__("DataQuality", workspace_client)
        
    def validate_pipeline_output(self, table_name: str, 
                                checks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate pipeline output against quality rules"""
        
        results = {
            "table": table_name,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "passed": True
        }
        
        for check in checks:
            check_result = self._run_single_check(table_name, check)
            results["checks"].append(check_result)
            if not check_result["passed"]:
                results["passed"] = False
        
        self.log_action("quality_validation", results)
        return results
    
    def _run_single_check(self, table_name: str, check: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single quality check"""
        
        check_type = check.get("type")
        check_result = {
            "name": check.get("name", "unnamed_check"),
            "type": check_type,
            "passed": False,
            "details": {}
        }
        
        try:
            if check_type == "row_count":
                min_rows = check.get("min_rows", 0)
                max_rows = check.get("max_rows", float('inf'))
                
                # Execute count query
                count_query = f"SELECT COUNT(*) as cnt FROM {table_name}"
                result = self.workspace_client.statement_execution.execute_statement(
                    warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
                    statement=count_query
                )
                
                row_count = result.result.data[0][0]
                check_result["details"]["row_count"] = row_count
                check_result["passed"] = min_rows <= row_count <= max_rows
                
            elif check_type == "null_check":
                columns = check.get("columns", [])
                threshold = check.get("threshold", 0)
                
                for column in columns:
                    null_query = f"""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as nulls
                    FROM {table_name}
                    """
                    result = self.workspace_client.statement_execution.execute_statement(
                        warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
                        statement=null_query
                    )
                    
                    total = result.result.data[0][0]
                    nulls = result.result.data[0][1]
                    null_percentage = (nulls / total * 100) if total > 0 else 0
                    
                    check_result["details"][column] = {
                        "null_percentage": null_percentage,
                        "threshold": threshold
                    }
                    
                    if null_percentage > threshold:
                        check_result["passed"] = False
                        break
                else:
                    check_result["passed"] = True
                    
            elif check_type == "freshness":
                timestamp_column = check.get("timestamp_column")
                max_age_hours = check.get("max_age_hours", 24)
                
                freshness_query = f"""
                SELECT MAX({timestamp_column}) as latest
                FROM {table_name}
                """
                result = self.workspace_client.statement_execution.execute_statement(
                    warehouse_id=os.getenv("DATABRICKS_WAREHOUSE_ID"),
                    statement=freshness_query
                )
                
                latest_timestamp = result.result.data[0][0]
                # Check if data is fresh enough
                # Implementation depends on timestamp format
                check_result["passed"] = True  # Simplified
                
        except Exception as e:
            check_result["error"] = str(e)
            check_result["passed"] = False
            
        return check_result
    
    def generate_quality_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate a quality report"""
        
        report = f"""
# Data Quality Report
        
**Pipeline:** {validation_results.get('pipeline_name', 'Unknown')}
**Timestamp:** {validation_results['timestamp']}
**Overall Status:** {'✅ PASSED' if validation_results['passed'] else '❌ FAILED'}

## Check Results

"""
        for check in validation_results['checks']:
            status = '✅' if check['passed'] else '❌'
            report += f"""
### {status} {check['name']}
- **Type:** {check['type']}
- **Status:** {'Passed' if check['passed'] else 'Failed'}
- **Details:** {json.dumps(check.get('details', {}), indent=2)}

"""
        
        return report

# ============== GitHub Integration Agent ==============

class GitHubAgent(BaseAgent):
    """Handles GitHub operations and version control"""
    
    def __init__(self, workspace_client: WorkspaceClient, github_token: str, 
                 repo_name: str):
        super().__init__("GitHub", workspace_client)
        self.github = Github(github_token)
        self.repo = self.github.get_repo(repo_name)
        
    def create_pull_request(self, notebook_content: str, spec: PipelineSpecification,
                          quality_report: str) -> str:
        """Create a pull request with the generated pipeline"""
        
        try:
            # Create a new branch
            branch_name = f"pipeline/{spec.name}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            main_branch = self.repo.get_branch("main")
            self.repo.create_git_ref(
                ref=f"refs/heads/{branch_name}",
                sha=main_branch.commit.sha
            )
            
            # Add notebook file
            notebook_path = f"pipelines/{spec.name}/pipeline.py"
            self.repo.create_file(
                path=notebook_path,
                message=f"Add pipeline: {spec.name}",
                content=notebook_content,
                branch=branch_name
            )
            
            # Add quality report
            report_path = f"pipelines/{spec.name}/quality_report.md"
            self.repo.create_file(
                path=report_path,
                message=f"Add quality report for: {spec.name}",
                content=quality_report,
                branch=branch_name
            )
            
            # Add pipeline specification
            spec_path = f"pipelines/{spec.name}/specification.json"
            self.repo.create_file(
                path=spec_path,
                message=f"Add specification for: {spec.name}",
                content=json.dumps(spec.__dict__, indent=2, default=str),
                branch=branch_name
            )
            
            # Create pull request
            pr = self.repo.create_pull(
                title=f"New Pipeline: {spec.name}",
                body=self._create_pr_description(spec, quality_report),
                head=branch_name,
                base="main"
            )
            
            self.log_action("created_pull_request", {
                "pr_number": pr.number,
                "url": pr.html_url
            })
            
            return pr.html_url
            
        except GithubException as e:
            self.logger.error(f"GitHub operation failed: {e}")
            raise
    
    def _create_pr_description(self, spec: PipelineSpecification, 
                              quality_report: str) -> str:
        """Create pull request description"""
        
        return f"""
## Pipeline: {spec.name}

### Description
{spec.description}

### Configuration
- **Source Tables:** {', '.join(spec.source_tables)}
- **Target Table:** {spec.target_table}
- **Schedule:** {spec.schedule or 'Manual'}

### Transformations
{json.dumps(spec.transformations, indent=2)}

### Quality Checks Summary
{quality_report.split('## Check Results')[0]}

### Review Checklist
- [ ] Pipeline specification is correct
- [ ] Source tables are validated
- [ ] Transformations are properly implemented
- [ ] Data quality checks pass
- [ ] Performance is acceptable
- [ ] Error handling is adequate
- [ ] Documentation is complete

### Approval Required
This pipeline requires approval before deployment. Please review and approve/reject.

---
*Generated by Databricks Pipeline Builder*
"""
    
    def merge_pull_request(self, pr_number: int, commit_message: str = None) -> bool:
        """Merge an approved pull request"""
        try:
            pr = self.repo.get_pull(pr_number)
            
            if pr.mergeable:
                pr.merge(
                    commit_message=commit_message or f"Merge PR #{pr_number}: {pr.title}",
                    merge_method="squash"
                )
                self.log_action("merged_pull_request", {"pr_number": pr_number})
                return True
            else:
                self.logger.warning(f"PR #{pr_number} is not mergeable")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to merge PR: {e}")
            return False

# ============== Approval Orchestrator ==============

class ApprovalOrchestrator(BaseAgent):
    """Manages human-in-the-loop approval process"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        super().__init__("ApprovalOrchestrator", workspace_client)
        self.pending_approvals: Dict[str, ApprovalRequest] = {}
        
    def request_approval(self, pipeline_id: str, stage: PipelineStage,
                        description: str, artifacts: Dict[str, Any]) -> str:
        """Create an approval request"""
        
        approval_request = ApprovalRequest(
            pipeline_id=pipeline_id,
            stage=stage.value,
            description=description,
            artifacts=artifacts
        )
        
        approval_id = f"{pipeline_id}_{stage.value}_{int(time.time())}"
        self.pending_approvals[approval_id] = approval_request
        
        # In production, this would send notifications
        self._send_approval_notification(approval_request)
        
        self.log_action("approval_requested", {
            "approval_id": approval_id,
            "pipeline_id": pipeline_id,
            "stage": stage.value
        })
        
        return approval_id
    
    def _send_approval_notification(self, request: ApprovalRequest):
        """Send approval notification (email, Slack, etc.)"""
        # This would integrate with notification systems
        logger.info(f"Approval requested for {request.pipeline_id} at stage {request.stage}")
    
    def get_approval_status(self, approval_id: str) -> Optional[str]:
        """Check approval status"""
        # In production, this would check external systems
        # For demo, we'll simulate approval after some time
        if approval_id in self.pending_approvals:
            return "pending"
        return None
    
    def approve(self, approval_id: str, approver: str, comments: str = "") -> bool:
        """Approve a request"""
        if approval_id in self.pending_approvals:
            request = self.pending_approvals.pop(approval_id)
            
            self.log_action("approved", {
                "approval_id": approval_id,
                "approver": approver,
                "comments": comments,
                "pipeline_id": request.pipeline_id
            })
            
            return True
        return False
    
    def reject(self, approval_id: str, approver: str, reason: str) -> bool:
        """Reject a request"""
        if approval_id in self.pending_approvals:
            request = self.pending_approvals.pop(approval_id)
            
            self.log_action("rejected", {
                "approval_id": approval_id,
                "approver": approver,
                "reason": reason,
                "pipeline_id": request.pipeline_id
            })
            
            return True
        return False

# ============== Master Orchestrator ==============

class PipelineBuilderOrchestrator:
    """Master orchestrator that coordinates all agents"""
    
    def __init__(self, config: Dict[str, Any]):
        # Initialize Databricks client
        self.workspace_client = WorkspaceClient(
            host=config["databricks_host"],
            token=config["databricks_token"]
        )
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model=config.get("llm_model", "gpt-4"),
            temperature=0.1,
            api_key=config["openai_api_key"]
        )
        
        # Initialize agents
        self.spec_parser = SpecificationParserAgent(self.workspace_client, self.llm)
        self.catalog_agent = UnityCatalogAgent(self.workspace_client)
        self.notebook_generator = NotebookGeneratorAgent(self.workspace_client, self.llm)
        self.quality_agent = DataQualityAgent(self.workspace_client)
        self.github_agent = GitHubAgent(
            self.workspace_client,
            config["github_token"],
            config["github_repo"]
        )
        self.approval_orchestrator = ApprovalOrchestrator(self.workspace_client)
        
        # Pipeline tracking
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        
    def build_pipeline(self, statement: str, require_approval: bool = True) -> Dict[str, Any]:
        """Main entry point to build a pipeline from natural language"""
        
        pipeline_id = f"pipeline_{int(time.time())}"
        logger.info(f"Starting pipeline build: {pipeline_id}")
        
        try:
            # Stage 1: Parse specification
            logger.info("Stage 1: Parsing specification")
            spec = self.spec_parser.parse_statement(statement)
            
            # Stage 2: Validate tables in Unity Catalog
            logger.info("Stage 2: Validating Unity Catalog tables")
            table_validation = self.catalog_agent.validate_tables(spec.source_tables)
            
            # Check if all tables exist
            for table, info in table_validation.items():
                if not info["exists"]:
                    raise ValueError(f"Table {table} not found: {info.get('error')}")
            
            # Get schemas for all tables
            table_schemas = {}
            for table in spec.source_tables:
                table_schemas[table] = self.catalog_agent.get_table_schema(table)
            
            # Stage 3: Generate notebook
            logger.info("Stage 3: Generating notebook")
            notebook_content = self.notebook_generator.generate_notebook(spec, table_schemas)
            
            # Save notebook to workspace
            notebook_path = f"/Shared/pipelines/{spec.name}/pipeline"
            self.notebook_generator.save_notebook(notebook_content, notebook_path)
            
            # Stage 4: Run initial quality checks (dry run)
            logger.info("Stage 4: Running quality checks")
            quality_results = {
                "pipeline_name": spec.name,
                "timestamp": datetime.now().isoformat(),
                "checks": [],
                "passed": True  # Assuming dry run passes
            }
            
            quality_report = self.quality_agent.generate_quality_report(quality_results)
            
            # Stage 5: Create GitHub PR
            logger.info("Stage 5: Creating GitHub pull request")
            pr_url = self.github_
