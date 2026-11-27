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

export interface DiagnosticHypothesis {
  agent_id: string;
  root_cause_label: string;
  confidence: number;
  weight: number;
  evidence_summary: string;
}

export interface ConsensusReport {
  status: 'CONSENSUS_REACHED' | 'CONFLICT_FAIL_CLOSED';
  agreed_root_cause: string | null;
  top_candidate_root_cause: string;
  consensus_confidence: number;
  agreement_ratio: number;
  disagreement_score: number;
  confidence_threshold: number;
  max_disagreement: number;
  reason: string;
  label_scores: Record<string, number>;
  hypotheses: DiagnosticHypothesis[];
}

export interface AgentShapleyAttribution {
  agent_id: string;
  root_cause_label: string;
  confidence: number;
  supports_target: boolean;
  shapley_value: number;
  normalized_share: number;
}

export interface ShapleyAttributionReport {
  target_root_cause: string;
  coalition_count: number;
  empty_coalition_value: number;
  grand_coalition_value: number;
  efficiency_sum: number;
  efficiency_delta: number;
  coalition_values: Record<string, number>;
  attributions: AgentShapleyAttribution[];
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

export interface A2ASkillCard {
  id: string;
  name: string;
  description: string;
  permission: string;
  tags?: string[];
}

export interface A2AAgentCard {
  name: string;
  description: string;
  url: string;
  version: string;
  skills: A2ASkillCard[];
}

export interface A2APeerDiscoverResponse {
  base_url: string;
  card: A2AAgentCard;
}

export interface A2APeerOut {
  id: string | null;
  base_url: string;
  status: string;
  enabled: boolean;
  source: string;
  card: A2AAgentCard | null;
  error: string | null;
  created_by?: string | null;
  created_at?: string | null;
}

export interface NLParseResponse {
  query: string;
  fault_type: string;
  service_name: string;
  matched_runbook: string;
  target_a2a_peer: string | null;
  suggested_skill: string | null;
  severity: string;
  parsed_summary: string;
}
