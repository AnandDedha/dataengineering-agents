"""
Data Engineering Agents Application
A complete multi-agent system for automated data pipeline management
"""

import json
import logging
import asyncio
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import uuid

# Core dependencies
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from pydantic import BaseModel, Field


# =============================================================================
# MODELS & SCHEMAS
# =============================================================================

class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    REQUIRES_APPROVAL = "requires_approval"

class AgentRole(Enum):
    ORCHESTRATOR = "orchestrator"
    INGESTION = "ingestion"
    SCHEMA_MODELING = "schema_modeling"
    QUALITY_VALIDATION = "quality_validation"
    TRANSFORMATION = "transformation"
    OPS_SCHEDULING = "ops_scheduling"
    OBSERVABILITY = "observability"
    GOVERNANCE = "governance"

@dataclass
class Task:
    id: str
    role: AgentRole
    description: str
    status: TaskStatus
    input_data: Dict[str, Any]
    output_data: Dict[str, Any] = None
    error: str = None
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

class ProposedDDL(BaseModel):
    table_name: str = Field(description="Name of the table to create")
    schema: str = Field(description="Database schema")
    columns: List[Dict[str, str]] = Field(description="Column definitions")
    partitioning: Optional[str] = Field(description="Partitioning strategy")
    clustering: Optional[List[str]] = Field(description="Clustering keys")

class DBTModel(BaseModel):
    name: str = Field(description="dbt model name")
    sql: str = Field(description="SQL query for the model")
    materialization: str = Field(default="table", description="dbt materialization type")
    tests: List[str] = Field(default=[], description="Data quality tests")

class IngestionConfig(BaseModel):
    source_type: str = Field(description="Type of data source")
    connection_params: Dict[str, str] = Field(description="Connection parameters")
    schedule: str = Field(description="Cron schedule for ingestion")
    incremental_key: Optional[str] = Field(description="Column for incremental loads")

class AgentState(BaseModel):
    """Shared state across all agents"""
    user_request: str = ""
    current_task: Optional[Task] = None
    tasks: List[Task] = []
    plan: List[Dict[str, Any]] = []
    context: Dict[str, Any] = {}
    requires_approval: bool = False
    approval_request: str = ""
    artifacts: Dict[str, Any] = {}


# =============================================================================
# TOOLS
# =============================================================================

class DataTools:
    """Collection of data engineering tools that agents can use"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.audit_log = []
    
    def _audit(self, action: str, params: Dict[str, Any], user: str = "system"):
        """Log all tool calls for audit purposes"""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "params": params,
            "user": user,
            "id": str(uuid.uuid4())
        }
        self.audit_log.append(entry)
        self.logger.info(f"Tool call: {action} by {user}")
    
    def run_sql(self, query: str, dry_run: bool = True) -> Dict[str, Any]:
        """Execute SQL query with safety checks"""
        self._audit("run_sql", {"query": query[:100] + "...", "dry_run": dry_run})
        
        # Safety checks
        dangerous_keywords = ["DROP", "DELETE", "TRUNCATE", "ALTER"]
        if any(keyword in query.upper() for keyword in dangerous_keywords):
            if not dry_run:
                return {"error": "Destructive operation requires approval", "status": "blocked"}
        
        # Simulate query execution
        if dry_run:
            return {
                "status": "success",
                "message": "Query validated successfully",
                "estimated_cost": "$0.05",
                "row_count": 1250
            }
        else:
            return {
                "status": "success",
                "message": "Query executed successfully",
                "rows_affected": 1250
            }
    
    def create_table(self, ddl: ProposedDDL) -> Dict[str, Any]:
        """Create table with given DDL"""
        self._audit("create_table", {"table": ddl.table_name, "schema": ddl.schema})
        
        # Simulate table creation
        return {
            "status": "success",
            "message": f"Table {ddl.schema}.{ddl.table_name} created successfully",
            "table_name": f"{ddl.schema}.{ddl.table_name}"
        }
    
    def dbt_run(self, models: List[str] = None, test: bool = True) -> Dict[str, Any]:
        """Run dbt models with optional testing"""
        self._audit("dbt_run", {"models": models, "test": test})
        
        models = models or ["all"]
        result = {
            "status": "success",
            "models_run": len(models) if models != ["all"] else 12,
            "tests_passed": 45 if test else 0,
            "tests_failed": 2 if test else 0
        }
        
        if test and result["tests_failed"] > 0:
            result["test_failures"] = [
                "sales_model: null values found in customer_id",
                "inventory_model: duplicate records detected"
            ]
        
        return result
    
    def dbt_test(self, models: List[str] = None) -> Dict[str, Any]:
        """Run dbt tests"""
        self._audit("dbt_test", {"models": models})
        
        return {
            "status": "success",
            "tests_run": 28,
            "tests_passed": 26,
            "tests_failed": 2,
            "failures": [
                "unique_orders_order_id: 3 duplicate values",
                "not_null_customers_email: 1 null value"
            ]
        }
    
    def schedule_job(self, dag_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update Airflow DAG"""
        self._audit("schedule_job", dag_config)
        
        return {
            "status": "success",
            "dag_id": dag_config.get("dag_id", "generated_dag"),
            "message": "DAG scheduled successfully",
            "next_run": "2024-01-15 09:00:00"
        }
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get table schema information"""
        self._audit("get_table_schema", {"table": table_name})
        
        # Simulate schema discovery
        return {
            "table_name": table_name,
            "columns": [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "customer_name", "type": "STRING", "nullable": True},
                {"name": "order_date", "type": "DATE", "nullable": False},
                {"name": "amount", "type": "DECIMAL", "nullable": False}
            ],
            "row_count": 125000,
            "last_updated": "2024-01-14 18:30:00"
        }
    
    def estimate_query_cost(self, query: str) -> Dict[str, Any]:
        """Estimate query execution cost"""
        self._audit("estimate_query_cost", {"query": query[:50] + "..."})
        
        # Simple cost estimation based on query complexity
        query_lower = query.lower()
        base_cost = 0.01
        
        if "join" in query_lower:
            base_cost *= 2
        if "group by" in query_lower:
            base_cost *= 1.5
        if "order by" in query_lower:
            base_cost *= 1.2
        
        return {
            "estimated_cost": f"${base_cost:.3f}",
            "estimated_bytes": "2.3 GB",
            "estimated_runtime": "45 seconds"
        }


# =============================================================================
# AGENTS
# =============================================================================

class BaseAgent:
    """Base class for all data engineering agents"""
    
    def __init__(self, role: AgentRole, tools: DataTools):
        self.role = role
        self.tools = tools
        self.logger = logging.getLogger(f"agent.{role.value}")
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        """Process the current task and return updated state"""
        raise NotImplementedError


class OrchestratorAgent(BaseAgent):
    """Plans and coordinates work across all agents"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.ORCHESTRATOR, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        self.logger.info(f"Orchestrating request: {state.user_request}")
        
        # Parse user request and create execution plan
        if "shopify" in state.user_request.lower() and "snowflake" in state.user_request.lower():
            plan = [
                {"agent": AgentRole.INGESTION, "action": "setup_shopify_connector", "priority": 1},
                {"agent": AgentRole.SCHEMA_MODELING, "action": "design_staging_schema", "priority": 2},
                {"agent": AgentRole.QUALITY_VALIDATION, "action": "create_data_tests", "priority": 3},
                {"agent": AgentRole.TRANSFORMATION, "action": "build_sales_models", "priority": 4},
                {"agent": AgentRole.OPS_SCHEDULING, "action": "create_daily_schedule", "priority": 5},
                {"agent": AgentRole.OBSERVABILITY, "action": "setup_monitoring", "priority": 6}
            ]
        else:
            # Generic plan
            plan = [
                {"agent": AgentRole.SCHEMA_MODELING, "action": "analyze_requirements", "priority": 1},
                {"agent": AgentRole.TRANSFORMATION, "action": "build_models", "priority": 2}
            ]
        
        # Create tasks for each step
        tasks = []
        for step in plan:
            task = Task(
                id=str(uuid.uuid4()),
                role=step["agent"],
                description=step["action"],
                status=TaskStatus.PENDING,
                input_data={"user_request": state.user_request, "context": state.context}
            )
            tasks.append(task)
        
        return {
            "plan": plan,
            "tasks": tasks,
            "current_task": tasks[0] if tasks else None,
            "context": {**state.context, "orchestration_complete": True}
        }


class IngestionAgent(BaseAgent):
    """Handles data ingestion from various sources"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.INGESTION, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        task = state.current_task
        self.logger.info(f"Processing ingestion task: {task.description}")
        
        if "shopify" in task.description.lower():
            # Configure Shopify connector
            config = IngestionConfig(
                source_type="shopify",
                connection_params={
                    "api_key": "{{vault.shopify_api_key}}",
                    "shop_name": "{{vault.shopify_shop_name}}",
                    "api_version": "2023-04"
                },
                schedule="0 2 * * *",  # Daily at 2 AM
                incremental_key="updated_at"
            )
            
            # Simulate connector setup
            result = {
                "status": "success",
                "connector_id": "shopify_prod_001",
                "tables_configured": ["orders", "customers", "products", "inventory"],
                "next_sync": "2024-01-15 02:00:00"
            }
            
            task.output_data = {
                "config": config.model_dump(),
                "result": result
            }
            task.status = TaskStatus.COMPLETED
        
        return {"current_task": task}


class SchemaModelingAgent(BaseAgent):
    """Designs and manages data schemas"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.SCHEMA_MODELING, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        task = state.current_task
        self.logger.info(f"Processing schema task: {task.description}")
        
        if "staging_schema" in task.description.lower():
            # Design staging tables for Shopify data
            staging_tables = [
                ProposedDDL(
                    table_name="stg_shopify_orders",
                    schema="staging",
                    columns=[
                        {"name": "order_id", "type": "INTEGER", "nullable": False},
                        {"name": "customer_id", "type": "INTEGER", "nullable": True},
                        {"name": "order_date", "type": "TIMESTAMP", "nullable": False},
                        {"name": "total_amount", "type": "DECIMAL(10,2)", "nullable": False},
                        {"name": "status", "type": "STRING", "nullable": False},
                        {"name": "_extracted_at", "type": "TIMESTAMP", "nullable": False}
                    ],
                    partitioning="DATE(order_date)",
                    clustering=["customer_id"]
                ),
                ProposedDDL(
                    table_name="stg_shopify_customers",
                    schema="staging",
                    columns=[
                        {"name": "customer_id", "type": "INTEGER", "nullable": False},
                        {"name": "email", "type": "STRING", "nullable": False},
                        {"name": "first_name", "type": "STRING", "nullable": True},
                        {"name": "last_name", "type": "STRING", "nullable": True},
                        {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
                    ]
                )
            ]
            
            # Create the tables
            created_tables = []
            for ddl in staging_tables:
                result = self.tools.create_table(ddl)
                created_tables.append(result)
            
            task.output_data = {
                "staging_tables": [ddl.model_dump() for ddl in staging_tables],
                "creation_results": created_tables
            }
            task.status = TaskStatus.COMPLETED
        
        return {"current_task": task}


class QualityValidationAgent(BaseAgent):
    """Creates and maintains data quality tests"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.QUALITY_VALIDATION, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        task = state.current_task
        self.logger.info(f"Processing quality task: {task.description}")
        
        if "data_tests" in task.description.lower():
            # Create comprehensive data quality tests
            tests = [
                {
                    "model": "stg_shopify_orders",
                    "tests": [
                        "unique:order_id",
                        "not_null:order_id,order_date,total_amount",
                        "accepted_values:status,['pending','paid','shipped','delivered','cancelled']",
                        "relationships:customer_id,ref('stg_shopify_customers'),customer_id"
                    ]
                },
                {
                    "model": "stg_shopify_customers",
                    "tests": [
                        "unique:customer_id",
                        "not_null:customer_id,email",
                        "unique:email"
                    ]
                }
            ]
            
            # Run the tests
            test_result = self.tools.dbt_test(models=["stg_shopify_orders", "stg_shopify_customers"])
            
            task.output_data = {
                "tests_created": tests,
                "test_results": test_result
            }
            
            if test_result["tests_failed"] > 0:
                task.status = TaskStatus.REQUIRES_APPROVAL
                state.requires_approval = True
                state.approval_request = f"Data quality tests failed: {test_result['failures']}"
            else:
                task.status = TaskStatus.COMPLETED
        
        return {
            "current_task": task,
            "requires_approval": state.requires_approval,
            "approval_request": state.approval_request
        }


class TransformationAgent(BaseAgent):
    """Builds dbt models and transformations"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.TRANSFORMATION, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        task = state.current_task
        self.logger.info(f"Processing transformation task: {task.description}")
        
        if "sales_models" in task.description.lower():
            # Build daily sales mart
            daily_sales_model = DBTModel(
                name="daily_sales",
                sql="""
                SELECT 
                    DATE(o.order_date) as sales_date,
                    COUNT(DISTINCT o.order_id) as order_count,
                    COUNT(DISTINCT o.customer_id) as unique_customers,
                    SUM(o.total_amount) as total_revenue,
                    AVG(o.total_amount) as avg_order_value
                FROM {{ ref('stg_shopify_orders') }} o
                WHERE o.status IN ('paid', 'shipped', 'delivered')
                GROUP BY DATE(o.order_date)
                """,
                materialization="table",
                tests=["not_null:sales_date,order_count,total_revenue"]
            )
            
            # Estimate cost before running
            cost_estimate = self.tools.estimate_query_cost(daily_sales_model.sql)
            
            # Run the model
            run_result = self.tools.dbt_run(models=["daily_sales"], test=True)
            
            task.output_data = {
                "model": daily_sales_model.model_dump(),
                "cost_estimate": cost_estimate,
                "run_result": run_result
            }
            task.status = TaskStatus.COMPLETED
        
        return {"current_task": task}


class OpsSchedulingAgent(BaseAgent):
    """Handles scheduling and orchestration"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.OPS_SCHEDULING, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        task = state.current_task
        self.logger.info(f"Processing ops task: {task.description}")
        
        if "daily_schedule" in task.description.lower():
            # Create daily pipeline DAG
            dag_config = {
                "dag_id": "daily_shopify_pipeline",
                "description": "Daily Shopify data pipeline",
                "schedule_interval": "0 2 * * *",  # 2 AM daily
                "start_date": "2024-01-15",
                "tasks": [
                    {"task_id": "extract_shopify", "type": "ingestion", "depends_on": []},
                    {"task_id": "run_staging_models", "type": "dbt_run", "depends_on": ["extract_shopify"]},
                    {"task_id": "test_staging", "type": "dbt_test", "depends_on": ["run_staging_models"]},
                    {"task_id": "run_mart_models", "type": "dbt_run", "depends_on": ["test_staging"]},
                    {"task_id": "test_marts", "type": "dbt_test", "depends_on": ["run_mart_models"]}
                ]
            }
            
            schedule_result = self.tools.schedule_job(dag_config)
            
            task.output_data = {
                "dag_config": dag_config,
                "schedule_result": schedule_result
            }
            task.status = TaskStatus.COMPLETED
        
        return {"current_task": task}


class ObservabilityAgent(BaseAgent):
    """Monitors performance and costs"""
    
    def __init__(self, tools: DataTools):
        super().__init__(AgentRole.OBSERVABILITY, tools)
    
    async def process(self, state: AgentState) -> Dict[str, Any]:
        task = state.current_task
        self.logger.info(f"Processing observability task: {task.description}")
        
        if "monitoring" in task.description.lower():
            # Setup monitoring and alerting
            monitoring_config = {
                "cost_alerts": {
                    "daily_limit": "$10.00",
                    "weekly_limit": "$50.00",
                    "alert_channels": ["slack", "email"]
                },
                "performance_alerts": {
                    "max_runtime": "30 minutes",
                    "failure_threshold": "2 consecutive failures"
                },
                "data_freshness": {
                    "max_staleness": "25 hours",
                    "critical_tables": ["daily_sales", "stg_shopify_orders"]
                }
            }
            
            task.output_data = {
                "monitoring_config": monitoring_config,
                "status": "monitoring configured successfully"
            }
            task.status = TaskStatus.COMPLETED
        
        return {"current_task": task}


# =============================================================================
# WORKFLOW GRAPH
# =============================================================================

class DataAgentsWorkflow:
    """Main workflow orchestrator using LangGraph"""
    
    def __init__(self):
        self.tools = DataTools()
        self.agents = {
            AgentRole.ORCHESTRATOR: OrchestratorAgent(self.tools),
            AgentRole.INGESTION: IngestionAgent(self.tools),
            AgentRole.SCHEMA_MODELING: SchemaModelingAgent(self.tools),
            AgentRole.QUALITY_VALIDATION: QualityValidationAgent(self.tools),
            AgentRole.TRANSFORMATION: TransformationAgent(self.tools),
            AgentRole.OPS_SCHEDULING: OpsSchedulingAgent(self.tools),
            AgentRole.OBSERVABILITY: ObservabilityAgent(self.tools)
        }
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Build the graph
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        def orchestrator_node(state: Dict) -> Dict:
            agent_state = AgentState(**state)
            result = asyncio.run(self.agents[AgentRole.ORCHESTRATOR].process(agent_state))
            return {**state, **result}
        
        def agent_router(state: Dict) -> str:
            """Route to the next agent based on current task"""
            if not state.get("current_task"):
                if state.get("requires_approval"):
                    return "human_approval"
                return END
            
            current_task = Task(**state["current_task"])
            
            if current_task.status == TaskStatus.REQUIRES_APPROVAL:
                return "human_approval"
            elif current_task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                return "task_completed"
            else:
                return current_task.role.value
        
        def process_agent_task(agent_role: AgentRole):
            def agent_node(state: Dict) -> Dict:
                agent_state = AgentState(**state)
                result = asyncio.run(self.agents[agent_role].process(agent_state))
                return {**state, **result}
            return agent_node
        
        def task_completed_node(state: Dict) -> Dict:
            """Handle task completion and move to next task"""
            tasks = [Task(**t) for t in state.get("tasks", [])]
            current_task_id = state.get("current_task", {}).get("id")
            
            # Find next pending task
            next_task = None
            for task in tasks:
                if task.status == TaskStatus.PENDING:
                    next_task = task
                    break
            
            if next_task:
                return {**state, "current_task": asdict(next_task)}
            else:
                return {**state, "current_task": None}
        
        def human_approval_node(state: Dict) -> Dict:
            """Handle human approval requirements"""
            approval_request = state.get("approval_request", "")
            self.logger.warning(f"HUMAN APPROVAL REQUIRED: {approval_request}")
            
            # For demo purposes, auto-approve
            current_task = Task(**state["current_task"])
            current_task.status = TaskStatus.COMPLETED
            
            return {
                **state,
                "current_task": asdict(current_task),
                "requires_approval": False,
                "approval_request": ""
            }
        
        # Build the graph
        workflow = StateGraph(dict)
        
        # Add nodes
        workflow.add_node("orchestrator", orchestrator_node)
        workflow.add_node("task_completed", task_completed_node)
        workflow.add_node("human_approval", human_approval_node)
        
        # Add agent nodes
        for role in [AgentRole.INGESTION, AgentRole.SCHEMA_MODELING, 
                    AgentRole.QUALITY_VALIDATION, AgentRole.TRANSFORMATION,
                    AgentRole.OPS_SCHEDULING, AgentRole.OBSERVABILITY]:
            workflow.add_node(role.value, process_agent_task(role))
        
        # Add edges
        workflow.set_entry_point("orchestrator")
        workflow.add_conditional_edges("orchestrator", agent_router)
        workflow.add_conditional_edges("task_completed", agent_router)
        workflow.add_conditional_edges("human_approval", agent_router)
        
        # Add edges from each agent back to router
        for role in [AgentRole.INGESTION, AgentRole.SCHEMA_MODELING,
                    AgentRole.QUALITY_VALIDATION, AgentRole.TRANSFORMATION,
                    AgentRole.OPS_SCHEDULING, AgentRole.OBSERVABILITY]:
            workflow.add_conditional_edges(role.value, agent_router)
        
        return workflow.compile(checkpointer=MemorySaver())
    
    async def run(self, user_request: str) -> Dict[str, Any]:
        """Execute the data engineering workflow"""
        initial_state = {
            "user_request": user_request,
            "tasks": [],
            "plan": [],
            "context": {},
            "requires_approval": False,
            "approval_request": "",
            "artifacts": {}
        }
        
        self.logger.info(f"Starting workflow for request: {user_request}")
        
        # Run the graph
        config = {"thread_id": str(uuid.uuid4())}
        result = await self.graph.ainvoke(initial_state, config)
        
        return result
    
    def get_audit_log(self) -> List[Dict[str, Any]]:
        """Get the complete audit log of all operations"""
        return self.tools.audit_log


# =============================================================================
# MAIN APPLICATION
# =============================================================================

async def main():
    """Demo the data agents application"""
    
    # Create the workflow
    workflow = DataAgentsWorkflow()
    
    # Example requests
    requests = [
        "ingest Shopify data → clean → load to Snowflake → build daily sales model",
        "Build a customer analytics pipeline from our database",
        "Set up data quality monitoring for our sales tables"
    ]
    
    # Process each request
    for i, request in enumerate(requests[:1], 1):  # Just run first one for demo
        print(f"\n{'='*60}")
        print(f"REQUEST {i}: {request}")
        print('='*60)
        
        result = await workflow.run(request)
        
        print("\n📋 EXECUTION PLAN:")
        for step in result.get("plan", []):
            print(f"  {step['priority']}. {step['agent'].value}: {step['action']}")
        
        print("\n📊 TASK RESULTS:")
        tasks = result.get("tasks", [])
        for task_data in tasks:
            task = Task(**task_data)
            status_emoji = {
                TaskStatus.COMPLETED: "✅",
                TaskStatus.FAILED: "❌",
                TaskStatus.PENDING: "⏳",
                TaskStatus.REQUIRES_APPROVAL: "👤"
            }.get(task.status, "❓")
            
            print(f"  {status_emoji} {task.role.value}: {task.description}")
            if task.output_data:
                print(f"      Output: {list(task.output_data.keys())}")
        
        print(f"\n📈 AUDIT LOG ({len(workflow.get_audit_log())} operations):")
        for entry in workflow.get_audit_log()[-5:]:  # Show last 5
            print(f"  {entry['timestamp'][:19]} | {entry['action']} | {entry['user']}")
        
        if result.get("requires_approval"):
            print(f"\n⚠️  APPROVAL REQUIRED: {result['approval_request']}")


if __name__ == "__main__":
    asyncio.run(main())
