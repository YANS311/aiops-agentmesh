export interface HealthResponse {
  status: string;
  postgres: string;
  redis: string;
}

export type RoleType = 'VIEWER' | 'SRE_APPROVER';

export interface ActorInfo {
  actor_id: string;
  role: RoleType;
}

export interface SkillOut {
  skill_id: string;
  fault_type: string;
  title: string;
  description: string;
  default_severity: string;
  default_service: string;
  diagnostic_tools: Record<string, string>;
  remediation_tool: string;
  risk_level: string;
}

export interface AgentTask {
  id: string;
  incident_id: string;
  agent_role: string;
  step_order: number;
  action_type: string;
  status: string;
  input_payload: Record<string, unknown>;
  output_payload: Record<string, unknown>;
  worker_id: string | null;
  lease_owner: string | null;
  lease_expires_at: string | null;
  attempt_count: number;
  max_attempts: number;
  error_message: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface RemediationApproval {
  id: string;
  incident_id: string;
  task_id: string;
  skill_id: string;
  risk_level: string;
  proposed_command: string;
  action_plan: Record<string, unknown>;
  status: string;
  approved_by: string | null;
  approved_by_role: string | null;
  approval_comment: string | null;
  decided_at: string | null;
  executed_at: string | null;
  execution_result: Record<string, unknown> | null;
  created_at: string;
}

export interface TraceEvent {
  id: number;
  incident_id: string;
  agent_role: string;
  event_type: string;
  risk_tier: string;
  summary: string;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface IncidentTrajectory {
  id: string;
  incident_id: string;
  skill_id: string;
  fault_type: string;
  root_cause: string;
  resolution_command: string;
  trajectory_doc: Record<string, unknown>;
  created_at: string;
}

export interface IncidentSummary {
  id: string;
  idempotency_key: string | null;
  title: string;
  severity: string;
  service_name: string;
  fault_type: string;
  status: string;
  alert_payload: Record<string, unknown>;
  blackboard: Record<string, unknown>;
  root_cause_summary: string | null;
  version: number;
  created_at: string;
  updated_at: string;
  resolved_at: string | null;
}

export interface IncidentDetail extends IncidentSummary {
  tasks: AgentTask[];
  approval: RemediationApproval | null;
  trajectory: IncidentTrajectory | null;
}
