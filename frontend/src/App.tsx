import { useCallback, useEffect, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  Archive,
  CheckCircle2,
  Database,
  Play,
  ShieldAlert,
  ShieldCheck,
  Terminal,
  XCircle,
} from 'lucide-react';
import {
  ROLE_TOKENS,
  approveIncident,
  fetchCurrentActor,
  fetchHealth,
  fetchIncidentDetail,
  fetchIncidents,
  fetchSkills,
  injectFault,
  rejectIncident,
} from './api';
import type {
  ActorInfo,
  HealthResponse,
  IncidentDetail,
  IncidentSummary,
  RoleType,
  SkillOut,
  TraceEvent,
} from './types';

const STATUS_BADGE_STYLES: Record<string, string> = {
  OPEN: 'bg-sky-500/20 text-sky-300 border-sky-500/40',
  TRIAGING: 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40',
  DIAGNOSING: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40',
  PENDING_APPROVAL: 'bg-amber-500/20 text-amber-300 border-amber-500/50',
  EXECUTING_REMEDIATION: 'bg-purple-500/20 text-purple-300 border-purple-500/40',
  RESOLVED: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/50',
  REJECTED: 'bg-rose-500/20 text-rose-300 border-rose-500/40',
  FAILED: 'bg-red-500/20 text-red-300 border-red-500/50',
};

const REFRESH_TRIGGER_EVENTS = new Set([
  'SKILL_MATCHED',
  'TOOL_RESULT',
  'REMEDIATION_PROPOSED',
  'APPROVAL_REQUIRED',
  'APPROVAL_GRANTED',
  'HIGH_RISK_EXECUTED',
  'TRAJECTORY_ARCHIVED',
  'INCIDENT_RESOLVED',
  'APPROVAL_REJECTED',
  'INCIDENT_FAILED',
]);

export default function App() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [skills, setSkills] = useState<SkillOut[]>([]);
  const [selectedFault, setSelectedFault] = useState<string>('db_connection_pool_exhausted');
  const [incidents, setIncidents] = useState<IncidentSummary[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<IncidentDetail | null>(null);
  const [traces, setTraces] = useState<TraceEvent[]>([]);
  const [operatorName, setOperatorName] = useState<string>('sre-lead-01');
  const [approvalComment, setApprovalComment] = useState<string>(
    '已核对只读诊断证据，确认安全并放行执行。'
  );
  const [activeRole, setActiveRole] = useState<RoleType>('VIEWER');
  const [actorInfo, setActorInfo] = useState<ActorInfo | null>(null);
  const [rbacBanner, setRbacBanner] = useState<string | null>(null);
  const [busyAction, setBusyAction] = useState<boolean>(false);
  const [uiError, setUiError] = useState<string | null>(null);

  const refreshListAndDetail = useCallback(async (targetId: string | null) => {
    try {
      const list = await fetchIncidents();
      setIncidents(list);
      if (targetId) {
        const d = await fetchIncidentDetail(targetId);
        setDetail(d);
      }
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    let active = true;
    (async () => {
      try {
        const [h, s, list] = await Promise.all([
          fetchHealth(),
          fetchSkills(),
          fetchIncidents(),
        ]);
        if (!active) return;
        setHealth(h);
        setSkills(s);
        setIncidents(list);
        if (list.length > 0) {
          setSelectedId(list[0].id);
        }
      } catch (err) {
        if (active) {
          setUiError(err instanceof Error ? err.message : String(err));
        }
      }
    })();
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    let active = true;
    const token = ROLE_TOKENS[activeRole];
    void fetchCurrentActor(token)
      .then((info) => {
        if (!active) return;
        setActorInfo(info);
        setOperatorName(info.actor_id);
        setRbacBanner(null);
      })
      .catch((err) => {
        if (active) {
          setUiError(err instanceof Error ? err.message : String(err));
        }
      });
    return () => {
      active = false;
    };
  }, [activeRole]);

  useEffect(() => {
    if (!selectedId) {
      setDetail(null);
      setTraces([]);
      return;
    }

    let cancelled = false;
    setTraces([]);

    void refreshListAndDetail(selectedId);

    const es = new EventSource(`/api/v1/incidents/${selectedId}/events`);

    es.addEventListener('trace', (ev: MessageEvent<string>) => {
      if (cancelled) return;
      const parsed = JSON.parse(ev.data) as TraceEvent;
      setTraces((prev) => {
        if (prev.some((item) => item.id === parsed.id)) {
          return prev;
        }
        return [...prev, parsed];
      });
      if (REFRESH_TRIGGER_EVENTS.has(parsed.event_type)) {
        void refreshListAndDetail(selectedId);
      }
    });

    es.addEventListener('complete', () => {
      if (!cancelled) {
        void refreshListAndDetail(selectedId);
      }
      es.close();
    });

    es.onerror = () => {
      if (es.readyState === EventSource.CLOSED) {
        es.close();
      }
    };

    return () => {
      cancelled = true;
      es.close();
    };
  }, [selectedId, refreshListAndDetail]);

  const handleInjectFault = async () => {
    setBusyAction(true);
    setUiError(null);
    try {
      const created = await injectFault(selectedFault);
      setSelectedId(created.id);
      await refreshListAndDetail(created.id);
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleApprove = async () => {
    if (!selectedId) return;
    setUiError(null);
    if (activeRole !== 'SRE_APPROVER') {
      setRbacBanner(
        `权限不足（RBAC 拦截）：当前身份 ${actorInfo?.actor_id ?? 'viewer-demo-01'} (VIEWER) 无权执行高危审批，请先在顶栏切换为 SRE_APPROVER。`
      );
      return;
    }
    setRbacBanner(null);
    setBusyAction(true);
    try {
      const updated = await approveIncident(
        selectedId,
        actorInfo?.actor_id ?? operatorName,
        ROLE_TOKENS[activeRole],
        approvalComment.trim() || undefined
      );
      setDetail(updated);
      await refreshListAndDetail(selectedId);
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleReject = async () => {
    if (!selectedId) return;
    setUiError(null);
    if (activeRole !== 'SRE_APPROVER') {
      setRbacBanner(
        `权限不足（RBAC 拦截）：当前身份 ${actorInfo?.actor_id ?? 'viewer-demo-01'} (VIEWER) 无权拒绝高危审批，请先在顶栏切换为 SRE_APPROVER。`
      );
      return;
    }
    setRbacBanner(null);
    setBusyAction(true);
    try {
      const updated = await rejectIncident(
        selectedId,
        actorInfo?.actor_id ?? operatorName,
        ROLE_TOKENS[activeRole],
        '已由人工操作员拒绝执行'
      );
      setDetail(updated);
      await refreshListAndDetail(selectedId);
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const evidence = (detail?.blackboard?.evidence ?? {}) as Record<string, unknown>;
  const matchedSkill = (detail?.blackboard?.matched_skill ?? null) as Record<string, string> | null;

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col">
      {/* Top Control Bar */}
      <header className="border-b border-slate-800 bg-slate-900/80 backdrop-blur px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="h-9 w-9 rounded-lg bg-indigo-500/20 border border-indigo-400/40 flex items-center justify-center">
              <Activity className="h-5 w-5 text-indigo-400" />
            </div>
            <div>
              <h1 className="text-lg font-semibold tracking-tight text-white">
                AgentMesh AIOps 智能排障控制台
              </h1>
              <p className="text-xs text-slate-400">
                PG16 SKIP LOCKED 事务队列 • Redis Streams（Consumer Group + ACK）• Fail-Closed 人工审批门禁
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            {/* RBAC Role Switcher */}
            <div
              data-testid="rbac-role-switcher"
              className="flex items-center rounded-lg bg-slate-800 border border-slate-700 p-0.5 text-xs"
            >
              <button
                type="button"
                data-testid="role-viewer-btn"
                onClick={() => setActiveRole('VIEWER')}
                className={`px-2.5 py-1 rounded-md font-medium transition cursor-pointer ${
                  activeRole === 'VIEWER'
                    ? 'bg-amber-500/25 text-amber-200 border border-amber-400/40'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                VIEWER
              </button>
              <button
                type="button"
                data-testid="role-approver-btn"
                onClick={() => setActiveRole('SRE_APPROVER')}
                className={`px-2.5 py-1 rounded-md font-medium transition cursor-pointer ${
                  activeRole === 'SRE_APPROVER'
                    ? 'bg-emerald-500/25 text-emerald-200 border border-emerald-400/40'
                    : 'text-slate-400 hover:text-slate-200'
                }`}
              >
                SRE_APPROVER
              </button>
            </div>

            <div
              data-testid="health-badge"
              className="flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium bg-slate-800 border border-slate-700"
            >
              <Database className="h-3.5 w-3.5 text-emerald-400" />
              <span>
                {health
                  ? `PG16: ${health.postgres} | Redis: ${health.redis}`
                  : '正在检查基础设施...'}
              </span>
            </div>

            <select
              aria-label="故障场景预设"
              value={selectedFault}
              onChange={(e) => setSelectedFault(e.target.value)}
              className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            >
              {skills.length > 0 ? (
                skills.map((sk) => (
                  <option key={sk.fault_type} value={sk.fault_type}>
                    [{sk.default_severity}] {sk.fault_type}
                  </option>
                ))
              ) : (
                <option value="db_connection_pool_exhausted">
                  [P0] db_connection_pool_exhausted
                </option>
              )}
            </select>

            <button
              type="button"
              data-testid="inject-fault-btn"
              onClick={handleInjectFault}
              disabled={busyAction}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-sm font-medium text-white transition cursor-pointer"
            >
              <Play className="h-4 w-4" />
              注入故障告警
            </button>
          </div>
        </div>
      </header>

      {uiError && (
        <div className="max-w-7xl w-full mx-auto px-6 pt-4">
          <div className="rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            {uiError}
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="max-w-7xl w-full mx-auto flex-1 grid grid-cols-1 lg:grid-cols-12 gap-6 p-6">
        {/* Left Incident Queue Sidebar */}
        <aside className="lg:col-span-4 flex flex-col gap-3">
          <div className="flex items-center justify-between">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              故障事件列表 ({incidents.length})
            </h2>
          </div>

          <div
            data-testid="incident-list"
            className="flex flex-col gap-2.5 max-h-[75vh] overflow-y-auto pr-1"
          >
            {incidents.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-800 p-6 text-center text-sm text-slate-500">
                暂无故障单，点击右上角 <strong>注入故障告警</strong> 启动多智能体协同排障。
              </div>
            ) : (
              incidents.map((inc) => {
                const isSelected = inc.id === selectedId;
                return (
                  <button
                    key={inc.id}
                    type="button"
                    onClick={() => setSelectedId(inc.id)}
                    className={`text-left rounded-xl border p-3.5 transition cursor-pointer ${
                      isSelected
                        ? 'bg-slate-800/90 border-indigo-500/70 shadow-lg shadow-indigo-950/30'
                        : 'bg-slate-900/60 border-slate-800 hover:border-slate-700'
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2 mb-1.5">
                      <span className="text-xs font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700">
                        {inc.severity} • {inc.service_name}
                      </span>
                      <span
                        className={`text-[11px] font-semibold px-2 py-0.5 rounded-full border ${
                          STATUS_BADGE_STYLES[inc.status] ?? 'bg-slate-700 text-slate-200'
                        }`}
                      >
                        {inc.status}
                      </span>
                    </div>
                    <div className="text-sm font-medium text-slate-100 line-clamp-2">
                      {inc.title}
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </aside>

        {/* Right Incident Detail Workspace */}
        <section className="lg:col-span-8 flex flex-col gap-5">
          {!detail ? (
            <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-12 text-center text-slate-400">
              请从左侧选择一个故障事件，或点击右上角注入新故障告警以查看实时多智能体排障过程。
            </div>
          ) : (
            <>
              {/* Incident Summary & State Machine Header */}
              <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div>
                    <div className="flex items-center gap-2 text-xs text-slate-400 mb-1">
                      <span className="font-mono">{detail.id}</span>
                      {matchedSkill && (
                        <span className="px-2 py-0.5 rounded bg-indigo-500/15 text-indigo-300 border border-indigo-500/30">
                          Runbook Skill: {matchedSkill.skill_id}
                        </span>
                      )}
                    </div>
                    <h2 className="text-xl font-semibold text-white">{detail.title}</h2>
                  </div>

                  <span
                    data-testid="incident-status-badge"
                    className={`px-3 py-1 rounded-full text-xs font-bold tracking-wide border ${
                      STATUS_BADGE_STYLES[detail.status] ?? 'bg-slate-700 text-slate-200'
                    }`}
                  >
                    {detail.status}
                  </span>
                </div>

                {detail.root_cause_summary && (
                  <div className="mt-4 rounded-lg bg-slate-950/70 border border-slate-800 p-3.5 text-sm text-slate-200">
                    <span className="font-semibold text-indigo-300">根因分析：</span>
                    {detail.root_cause_summary}
                  </div>
                )}

                {/* Agent Task Progress Strip */}
                <div className="mt-4 grid grid-cols-2 sm:grid-cols-5 gap-2">
                  {detail.tasks.map((t) => (
                    <div
                      key={t.id}
                      className="rounded-lg border border-slate-800 bg-slate-950/60 px-2.5 py-2"
                    >
                      <div className="text-[11px] font-semibold text-slate-300 truncate">
                        {t.agent_role}
                      </div>
                      <div className="text-[11px] text-slate-400 mt-0.5">{t.status}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Human-in-the-Loop Fail-Closed Approval Gate */}
              {detail.status === 'PENDING_APPROVAL' && detail.approval?.status === 'PENDING' && (
                <div
                  data-testid="approval-card"
                  className="rounded-xl border-2 border-amber-500/60 bg-amber-950/20 p-5 shadow-lg shadow-amber-950/20"
                >
                  <div className="flex items-center gap-2.5 text-amber-300 font-semibold text-base mb-2">
                    <ShieldAlert className="h-5 w-5" />
                    <span>高危写操作门禁 — 需人工审批放行（Fail-Closed）</span>
                  </div>
                  <p className="text-xs text-amber-200/80 mb-3">
                    只读诊断已自动放行；当前{' '}
                    <span className="font-semibold">{detail.approval.risk_level}</span> 级高危写入操作已强制挂起，等待运维人员确认后方可执行。
                  </p>

                  <div className="rounded-lg bg-slate-950 border border-slate-800 p-3 mb-3">
                    <div className="text-[11px] uppercase tracking-wider text-slate-400 mb-1 flex items-center gap-1.5">
                      <Terminal className="h-3.5 w-3.5 text-amber-400" />
                      待执行修复命令
                    </div>
                    <pre className="text-xs font-mono text-emerald-300 whitespace-pre-wrap">
                      {detail.approval.proposed_command}
                    </pre>
                  </div>

                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 mb-4">
                    <div>
                      <label className="block text-xs text-slate-400 mb-1">审批人</label>
                      <input
                        type="text"
                        value={operatorName}
                        onChange={(e) => setOperatorName(e.target.value)}
                        className="w-full rounded-lg bg-slate-900 border border-slate-700 px-3 py-1.5 text-sm text-white"
                      />
                    </div>
                    <div>
                      <label className="block text-xs text-slate-400 mb-1">审批备注</label>
                      <input
                        type="text"
                        value={approvalComment}
                        onChange={(e) => setApprovalComment(e.target.value)}
                        className="w-full rounded-lg bg-slate-900 border border-slate-700 px-3 py-1.5 text-sm text-white"
                      />
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center gap-3">
                    <button
                      type="button"
                      data-testid="approve-btn"
                      onClick={handleApprove}
                      disabled={busyAction}
                      className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 disabled:opacity-50 text-sm font-semibold text-white transition cursor-pointer"
                    >
                      <ShieldCheck className="h-4 w-4" />
                      批准并执行修复
                    </button>

                    <button
                      type="button"
                      data-testid="reject-btn"
                      onClick={handleReject}
                      disabled={busyAction}
                      className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-rose-600/80 hover:bg-rose-500 disabled:opacity-50 text-sm font-semibold text-white transition cursor-pointer"
                    >
                      <XCircle className="h-4 w-4" />
                      拒绝执行
                    </button>
                  </div>

                  {rbacBanner && (
                    <div
                      data-testid="rbac-denied-banner"
                      className="mt-3 rounded-lg border border-rose-500/50 bg-rose-950/40 px-3.5 py-2.5 text-xs text-rose-200"
                    >
                      {rbacBanner}
                    </div>
                  )}
                </div>
              )}

              {/* Live SSE Execution Trace Stream */}
              <div className="rounded-xl border border-slate-800 bg-slate-900/70 p-5">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                    <AlertTriangle className="h-4 w-4 text-indigo-400" />
                    多智能体实时执行轨迹（SSE）
                  </h3>
                  <span className="text-xs text-slate-400">{traces.length} 条事件</span>
                </div>

                <div
                  data-testid="trace-stream-list"
                  className="space-y-2 max-h-80 overflow-y-auto pr-1"
                >
                  {traces.map((ev) => (
                    <div
                      key={ev.id}
                      className="rounded-lg border border-slate-800 bg-slate-950/70 px-3.5 py-2.5 text-xs"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2 mb-1">
                        <div className="flex items-center gap-2">
                          <span className="font-mono font-semibold text-indigo-300">
                            {ev.agent_role}
                          </span>
                          <span className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-200 font-mono">
                            {ev.event_type}
                          </span>
                        </div>
                        <span
                          className={`px-2 py-0.5 rounded-full font-semibold ${
                            ev.risk_tier === 'HIGH_RISK_WRITE'
                              ? 'bg-amber-500/20 text-amber-300'
                              : ev.risk_tier === 'READ_ONLY'
                              ? 'bg-emerald-500/20 text-emerald-300'
                              : 'bg-slate-800 text-slate-400'
                          }`}
                        >
                          {ev.risk_tier}
                        </span>
                      </div>
                      <p className="text-slate-300">{ev.summary}</p>
                    </div>
                  ))}
                </div>
              </div>

              {/* Shared Blackboard Evidence & Archived Trajectory */}
              <div
                data-testid="trajectory-card"
                className="rounded-xl border border-slate-800 bg-slate-900/70 p-5 space-y-4"
              >
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                    <Archive className="h-4 w-4 text-emerald-400" />
                    共享黑板证据 &amp; 排障轨迹归档
                  </h3>
                  {detail.trajectory && (
                    <span className="inline-flex items-center gap-1 text-xs text-emerald-300 font-medium">
                      <CheckCircle2 className="h-3.5 w-3.5" />
                      Trajectory 已归档
                    </span>
                  )}
                </div>

                {Object.keys(evidence).length > 0 && (
                  <div>
                    <div className="text-xs font-medium text-slate-400 mb-1.5">
                      PG16 共享黑板只读诊断证据
                    </div>
                    <pre className="text-xs font-mono bg-slate-950 border border-slate-800 rounded-lg p-3 overflow-x-auto text-cyan-200">
                      {JSON.stringify(evidence, null, 2)}
                    </pre>
                  </div>
                )}

                {detail.trajectory && (
                  <div className="rounded-lg border border-emerald-500/40 bg-emerald-950/15 p-4 space-y-2">
                    <div className="text-xs font-semibold text-emerald-300">
                      Hermes 风格排障轨迹归档 • {detail.trajectory.skill_id}
                    </div>
                    <div className="text-xs text-slate-300">
                      <strong>审批人：</strong>{' '}
                      {detail.approval?.approved_by ?? 'N/A'}
                      {detail.approval?.approved_by_role
                        ? ` (${detail.approval.approved_by_role})`
                        : ''}{' '}
                      | <strong>执行命令：</strong>{' '}
                      <code className="text-emerald-300">{detail.trajectory.resolution_command}</code>
                    </div>
                    <pre className="text-[11px] font-mono bg-slate-950 border border-slate-800 rounded p-2.5 overflow-x-auto text-slate-300 max-h-48">
                      {JSON.stringify(detail.trajectory.trajectory_doc, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            </>
          )}
        </section>
      </main>
    </div>
  );
}
