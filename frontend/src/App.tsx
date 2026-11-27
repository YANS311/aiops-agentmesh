import { useCallback, useEffect, useState } from 'react';
import {
  Activity,
  AlertTriangle,
  Archive,
  BarChart3,
  CheckCircle2,
  Database,
  Globe,
  Play,
  Plus,
  Scale,
  Search,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  Terminal,
  Trash2,
  XCircle,
} from 'lucide-react';
import {
  ROLE_TOKENS,
  approveIncident,
  confirmNLIntent,
  deleteA2APeer,
  discoverA2APeer,
  fetchA2APeers,
  fetchCurrentActor,
  fetchHealth,
  fetchIncidentDetail,
  fetchIncidents,
  fetchSkills,
  injectFault,
  parseNLIntent,
  registerA2APeer,
  rejectIncident,
  subscribeIncidentEvents,
} from './api';
import type {
  A2APeerDiscoverResponse,
  A2APeerOut,
  ActorInfo,
  ConsensusReport,
  HealthResponse,
  IncidentDetail,
  IncidentSummary,
  NLParseResponse,
  RoleType,
  ShapleyAttributionReport,
  SkillOut,
  TraceEvent,
} from './types';

const STATUS_BADGE_STYLES: Record<string, string> = {
  OPEN: 'bg-sky-500/20 text-sky-300 border-sky-500/40',
  TRIAGING: 'bg-indigo-500/20 text-indigo-300 border-indigo-500/40',
  DIAGNOSING: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/40',
  NEEDS_HUMAN_REVIEW: 'bg-orange-500/20 text-orange-300 border-orange-500/50',
  PENDING_APPROVAL: 'bg-amber-500/20 text-amber-300 border-amber-500/50',
  EXECUTING_REMEDIATION: 'bg-purple-500/20 text-purple-300 border-purple-500/40',
  RESOLVED: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/50',
  REJECTED: 'bg-rose-500/20 text-rose-300 border-rose-500/40',
  FAILED: 'bg-red-500/20 text-red-300 border-red-500/50',
};

const OUTCOME_BADGE_STYLES: Record<string, { label: string; cls: string }> = {
  SIMULATED: {
    label: 'SIMULATED 模拟执行（未作用于真实生产）',
    cls: 'bg-sky-500/20 text-sky-200 border-sky-400/40',
  },
  VERIFIED: {
    label: 'VERIFIED 已后置验证',
    cls: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/50',
  },
  UNKNOWN: {
    label: 'UNKNOWN 待人工对账',
    cls: 'bg-orange-500/25 text-orange-200 border-orange-500/60',
  },
  FAILED: {
    label: 'FAILED 执行失败',
    cls: 'bg-red-500/25 text-red-200 border-red-500/60',
  },
};

const REFRESH_TRIGGER_EVENTS = new Set([
  'SKILL_MATCHED',
  'TOOL_RESULT',
  'NL_INCIDENT_CONFIRMED',
  'A2A_TASK_DELEGATED',
  'A2A_TASK_COMPLETED',
  'A2A_TASK_FAILED',
  'CONSENSUS_REACHED',
  'CONSENSUS_CONFLICT_ALERT',
  'REMEDIATION_PROPOSED',
  'APPROVAL_REQUIRED',
  'APPROVAL_GRANTED',
  'REMEDIATION_SIMULATED',
  'POST_CHECK_VERIFIED',
  'POST_CHECK_UNKNOWN_RECONCILE_REQUIRED',
  'POST_CHECK_FAILED',
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

  // Phase 6.1 A2A Peer Registration & NL Troubleshooting states
  const [a2aEndpoint, setA2aEndpoint] = useState<string>('http://127.0.0.1:8020');
  const [a2aPreview, setA2aPreview] = useState<A2APeerDiscoverResponse | null>(null);
  const [a2aPeers, setA2aPeers] = useState<A2APeerOut[]>([]);
  const [a2aNotice, setA2aNotice] = useState<string | null>(null);
  const [nlQuery, setNlQuery] = useState<string>('');
  const [nlDraft, setNlDraft] = useState<NLParseResponse | null>(null);

  const refreshListAndDetail = useCallback(
    async (targetId: string | null) => {
      const token = ROLE_TOKENS[activeRole];
      try {
        const list = await fetchIncidents(token);
        setIncidents(list);
        if (targetId) {
          const d = await fetchIncidentDetail(targetId, token);
          setDetail(d);
        }
      } catch (err) {
        setUiError(err instanceof Error ? err.message : String(err));
      }
    },
    [activeRole]
  );

  useEffect(() => {
    let active = true;
    const token = ROLE_TOKENS[activeRole];
    (async () => {
      try {
        const [h, s, list, peers] = await Promise.all([
          fetchHealth(),
          fetchSkills(token),
          fetchIncidents(token),
          fetchA2APeers(token),
        ]);
        if (!active) return;
        setHealth(h);
        setSkills(s);
        setIncidents(list);
        setA2aPeers(peers);
        if (list.length > 0) {
          setSelectedId((prev) => prev ?? list[0].id);
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
  }, [activeRole]);

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

    const token = ROLE_TOKENS[activeRole];
    const unsubscribe = subscribeIncidentEvents(
      selectedId,
      token,
      (parsed) => {
        if (cancelled) return;
        setTraces((prev) => {
          if (prev.some((item) => item.id === parsed.id)) {
            return prev;
          }
          return [...prev, parsed];
        });
        if (REFRESH_TRIGGER_EVENTS.has(parsed.event_type)) {
          void refreshListAndDetail(selectedId);
        }
      },
      () => {
        if (!cancelled) {
          void refreshListAndDetail(selectedId);
        }
      }
    );

    return () => {
      cancelled = true;
      unsubscribe();
    };
  }, [selectedId, activeRole, refreshListAndDetail]);

  const handleInjectFault = async () => {
    setBusyAction(true);
    setUiError(null);
    try {
      const created = await injectFault(selectedFault, ROLE_TOKENS[activeRole]);
      setSelectedId(created.id);
      await refreshListAndDetail(created.id);
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleDiscoverA2A = async () => {
    setBusyAction(true);
    setUiError(null);
    setA2aNotice(null);
    try {
      const preview = await discoverA2APeer(a2aEndpoint, ROLE_TOKENS[activeRole]);
      setA2aPreview(preview);
    } catch (err) {
      setA2aPreview(null);
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleRegisterA2A = async () => {
    setUiError(null);
    if (activeRole !== 'SRE_APPROVER') {
      setA2aNotice(
        `权限不足（RBAC 拦截）：当前身份 ${actorInfo?.actor_id ?? 'viewer-demo-01'} (VIEWER) 无权接入外部 A2A 节点，请先在顶栏切换为 SRE_APPROVER。`
      );
      return;
    }
    setBusyAction(true);
    setA2aNotice(null);
    try {
      await registerA2APeer(a2aEndpoint, ROLE_TOKENS[activeRole]);
      const updatedPeers = await fetchA2APeers(ROLE_TOKENS[activeRole]);
      setA2aPeers(updatedPeers);
      setA2aPreview(null);
      setA2aNotice(`已成功接入 A2A 节点：${a2aEndpoint}`);
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleDeleteA2A = async (peerId: string) => {
    if (activeRole !== 'SRE_APPROVER') {
      setA2aNotice('权限不足：删除 A2A 节点需切换为 SRE_APPROVER。');
      return;
    }
    setBusyAction(true);
    try {
      await deleteA2APeer(peerId, ROLE_TOKENS[activeRole]);
      setA2aPeers(await fetchA2APeers(ROLE_TOKENS[activeRole]));
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleParseNL = async () => {
    if (!nlQuery.trim()) return;
    setBusyAction(true);
    setUiError(null);
    try {
      const draft = await parseNLIntent(nlQuery.trim(), ROLE_TOKENS[activeRole]);
      setNlDraft(draft);
    } catch (err) {
      setUiError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusyAction(false);
    }
  };

  const handleConfirmNL = async () => {
    if (!nlDraft) return;
    setBusyAction(true);
    setUiError(null);
    try {
      const created = await confirmNLIntent(nlDraft, ROLE_TOKENS[activeRole]);
      setNlDraft(null);
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
  const consensusReport = ((detail?.trajectory?.trajectory_doc?.consensus_report ??
    detail?.blackboard?.consensus_report ??
    null) as ConsensusReport | null);
  const shapleyAttribution = ((detail?.trajectory?.trajectory_doc?.shapley_attribution ??
    detail?.blackboard?.shapley_attribution ??
    null) as ShapleyAttributionReport | null);
  const executionOutcome = ((detail?.approval?.execution_result?.execution_outcome ??
    detail?.blackboard?.execution_outcome ??
    null) as string | null);

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

      {/* Phase 6.1 Top Action Strip: A2A Peer Registration + Natural Language Troubleshooting */}
      <section className="max-w-7xl w-full mx-auto px-6 pt-5 grid grid-cols-1 lg:grid-cols-2 gap-5">
        {/* Panel 1: A2A External Agent Onboarding */}
        <div
          data-testid="a2a-onboarding-panel"
          className="rounded-xl border border-slate-800 bg-slate-900/70 p-4 space-y-3"
        >
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold text-white flex items-center gap-2">
              <Globe className="h-4 w-4 text-cyan-400" />
              A2A 外部智能体接入
            </h2>
            <span className="text-xs text-slate-400">已接入 {a2aPeers.length} 个节点</span>
          </div>

          <div className="flex flex-wrap gap-2">
            <input
              type="text"
              data-testid="a2a-endpoint-input"
              value={a2aEndpoint}
              onChange={(e) => setA2aEndpoint(e.target.value)}
              placeholder="http://127.0.0.1:8020"
              className="flex-1 min-w-[200px] rounded-lg bg-slate-950 border border-slate-700 px-3 py-1.5 text-xs text-slate-100 font-mono focus:outline-none focus:ring-2 focus:ring-cyan-500"
            />
            <button
              type="button"
              data-testid="a2a-discover-btn"
              onClick={handleDiscoverA2A}
              disabled={busyAction}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 border border-slate-600 text-xs font-medium text-slate-100 transition cursor-pointer"
            >
              <Search className="h-3.5 w-3.5 text-cyan-400" />
              探测 Agent Card
            </button>
          </div>

          {a2aPreview && (
            <div
              data-testid="a2a-preview-card"
              className="rounded-lg border border-cyan-500/40 bg-cyan-950/20 p-3 space-y-2 text-xs"
            >
              <div className="flex items-center justify-between gap-2">
                <div>
                  <span className="font-semibold text-cyan-200">{a2aPreview.card.name}</span>
                  <span className="ml-2 font-mono text-slate-400">v{a2aPreview.card.version}</span>
                </div>
                <button
                  type="button"
                  data-testid="a2a-register-btn"
                  onClick={handleRegisterA2A}
                  disabled={busyAction}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-semibold cursor-pointer"
                >
                  <Plus className="h-3.5 w-3.5" />
                  确认接入
                </button>
              </div>
              <p className="text-slate-300">{a2aPreview.card.description}</p>
              <div className="flex flex-wrap gap-1.5">
                {a2aPreview.card.skills.map((sk) => (
                  <span
                    key={sk.id}
                    className="px-2 py-0.5 rounded bg-slate-900 border border-cyan-500/30 text-cyan-300 font-mono text-[11px]"
                  >
                    {sk.id} ({sk.permission})
                  </span>
                ))}
              </div>
            </div>
          )}

          {a2aNotice && (
            <div className="rounded-lg border border-amber-500/40 bg-amber-950/30 px-3 py-2 text-xs text-amber-200">
              {a2aNotice}
            </div>
          )}

          <div data-testid="a2a-peers-list" className="space-y-1.5 max-h-28 overflow-y-auto">
            {a2aPeers.length === 0 ? (
              <div className="text-xs text-slate-500">
                暂无已注册外部 A2A 节点，输入 Endpoint 并探测即可接入。
              </div>
            ) : (
              a2aPeers.map((peer) => (
                <div
                  key={peer.id ?? peer.base_url}
                  className="flex items-center justify-between gap-2 rounded-lg border border-slate-800 bg-slate-950/60 px-2.5 py-1.5 text-xs"
                >
                  <div className="min-w-0">
                    <div className="font-mono text-slate-200 truncate">
                      {peer.card?.name ?? peer.base_url}
                    </div>
                    <div className="text-[11px] text-slate-400 truncate">{peer.base_url}</div>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <span
                      className={`px-2 py-0.5 rounded-full text-[10px] font-semibold border ${
                        peer.status === 'ONLINE'
                          ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                          : 'bg-slate-800 text-slate-400 border-slate-700'
                      }`}
                    >
                      {peer.status}
                    </span>
                    {peer.id && (
                      <button
                        type="button"
                        onClick={() => handleDeleteA2A(peer.id!)}
                        className="text-slate-500 hover:text-rose-400 cursor-pointer"
                        title="移除节点"
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </button>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Panel 2: Natural Language Troubleshooting */}
        <div
          data-testid="nl-troubleshooting-panel"
          className="rounded-xl border border-slate-800 bg-slate-900/70 p-4 space-y-3"
        >
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-sm font-semibold text-white flex items-center gap-2">
              <Sparkles className="h-4 w-4 text-indigo-400" />
              自然语言排障
            </h2>
            <div className="flex items-center gap-1.5">
              <button
                type="button"
                data-testid="nl-example-neo4j"
                onClick={() =>
                  setNlQuery('学院智能导学 Neo4j 知识图谱 Cypher 查询超时，连接池接近打满，请帮忙排查')
                }
                className="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-[11px] text-indigo-300 border border-slate-700 cursor-pointer"
              >
                示例：导学 Neo4j 慢查询
              </button>
              <button
                type="button"
                data-testid="nl-example-rag"
                onClick={() =>
                  setNlQuery('学院导学 RAG 检索延迟飙升，空召回率超过 30%，请立即诊断向量检索链路')
                }
                className="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-[11px] text-cyan-300 border border-slate-700 cursor-pointer"
              >
                示例：RAG 空召回
              </button>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <input
              type="text"
              data-testid="nl-query-input"
              value={nlQuery}
              onChange={(e) => setNlQuery(e.target.value)}
              placeholder="用自然语言描述线上故障，例如：导学 Neo4j 图谱慢查询..."
              className="flex-1 min-w-[220px] rounded-lg bg-slate-950 border border-slate-700 px-3 py-1.5 text-xs text-slate-100 focus:outline-none focus:ring-2 focus:ring-indigo-500"
            />
            <button
              type="button"
              data-testid="nl-parse-btn"
              onClick={handleParseNL}
              disabled={busyAction || !nlQuery.trim()}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-xs font-medium text-white transition cursor-pointer"
            >
              解析意图
            </button>
          </div>

          {nlDraft && (
            <div
              data-testid="nl-preview-card"
              className="rounded-lg border border-indigo-500/40 bg-indigo-950/20 p-3 space-y-2 text-xs"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="font-semibold text-indigo-200">
                  意图确认卡 • [{nlDraft.severity}] {nlDraft.service_name}
                </span>
                <button
                  type="button"
                  data-testid="nl-confirm-btn"
                  onClick={handleConfirmNL}
                  disabled={busyAction}
                  className="inline-flex items-center gap-1 px-2.5 py-1 rounded bg-emerald-600 hover:bg-emerald-500 text-white font-semibold cursor-pointer"
                >
                  <Play className="h-3.5 w-3.5" />
                  确认发起排障
                </button>
              </div>
              <p className="text-slate-300">{nlDraft.parsed_summary}</p>
              <div className="flex flex-wrap gap-2 text-[11px] font-mono text-slate-300">
                <span className="px-2 py-0.5 rounded bg-slate-900 border border-slate-700">
                  Runbook: {nlDraft.matched_runbook}
                </span>
                <span className="px-2 py-0.5 rounded bg-slate-900 border border-slate-700">
                  A2A 节点: {nlDraft.target_a2a_peer ?? '自动匹配'}
                </span>
                {nlDraft.suggested_skill && (
                  <span className="px-2 py-0.5 rounded bg-slate-900 border border-slate-700">
                    Skill: {nlDraft.suggested_skill}
                  </span>
                )}
              </div>
            </div>
          )}
        </div>
      </section>

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

                  <div className="flex flex-wrap items-center gap-2">
                    {executionOutcome && OUTCOME_BADGE_STYLES[executionOutcome] && (
                      <span
                        data-testid="execution-outcome-badge"
                        className={`px-2.5 py-1 rounded-full text-[11px] font-semibold border ${OUTCOME_BADGE_STYLES[executionOutcome].cls}`}
                      >
                        {OUTCOME_BADGE_STYLES[executionOutcome].label}
                      </span>
                    )}
                    <span
                      data-testid="incident-status-badge"
                      className={`px-3 py-1 rounded-full text-xs font-bold tracking-wide border ${
                        STATUS_BADGE_STYLES[detail.status] ?? 'bg-slate-700 text-slate-200'
                      }`}
                    >
                      {detail.status}
                    </span>
                  </div>
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

              {/* Multi-Agent Consensus Arbiter & Exact Shapley Attribution Panel */}
              {consensusReport && (
                <div
                  data-testid="consensus-shapley-panel"
                  className="rounded-xl border border-slate-800 bg-slate-900/70 p-5 space-y-4"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                      <Scale className="h-4 w-4 text-cyan-400" />
                      多智能体共识仲裁 &amp; Shapley 贡献度归因
                    </h3>
                    <div className="flex flex-wrap items-center gap-2 text-xs">
                      <span
                        data-testid="consensus-status-badge"
                        className={`px-2.5 py-0.5 rounded-full font-semibold border ${
                          consensusReport.status === 'CONSENSUS_REACHED'
                            ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                            : 'bg-orange-500/20 text-orange-300 border-orange-500/50'
                        }`}
                      >
                        {consensusReport.status === 'CONSENSUS_REACHED'
                          ? '共识达成 (CONSENSUS_REACHED)'
                          : '共识冲突拦截 (CONFLICT_FAIL_CLOSED)'}
                      </span>
                      <span className="px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700 font-mono">
                        置信度 {(consensusReport.consensus_confidence * 100).toFixed(1)}%
                      </span>
                      <span className="px-2 py-0.5 rounded bg-slate-800 text-slate-300 border border-slate-700 font-mono">
                        一致率 {(consensusReport.agreement_ratio * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>

                  <div
                    className={`rounded-lg border px-3.5 py-2.5 text-xs ${
                      consensusReport.status === 'CONSENSUS_REACHED'
                        ? 'border-cyan-500/30 bg-cyan-950/20 text-cyan-100'
                        : 'border-orange-500/40 bg-orange-950/30 text-orange-200'
                    }`}
                  >
                    <strong>仲裁结论：</strong>
                    {consensusReport.reason}
                  </div>

                  {/* Independent Specialist Hypotheses */}
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    {consensusReport.hypotheses.map((hyp) => (
                      <div
                        key={hyp.agent_id}
                        data-testid={`hypothesis-card-${hyp.agent_id}`}
                        className="rounded-lg border border-slate-800 bg-slate-950/70 p-3 space-y-1.5"
                      >
                        <div className="flex items-center justify-between gap-2">
                          <span className="text-xs font-mono font-semibold text-indigo-300">
                            {hyp.agent_id}
                          </span>
                          <span className="text-[11px] font-mono px-1.5 py-0.5 rounded bg-slate-800 text-emerald-300">
                            {(hyp.confidence * 100).toFixed(0)}%
                          </span>
                        </div>
                        <div className="text-[11px] font-mono text-cyan-300 truncate">
                          假设：{hyp.root_cause_label}
                        </div>
                        <p className="text-xs text-slate-300 leading-relaxed">
                          {hyp.evidence_summary}
                        </p>
                      </div>
                    ))}
                  </div>

                  {/* Exact Shapley Attribution Progress Bars */}
                  {shapleyAttribution && shapleyAttribution.attributions.length > 0 && (
                    <div
                      data-testid="shapley-attribution-section"
                      className="rounded-lg border border-slate-800 bg-slate-950/60 p-4 space-y-3"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="text-xs font-semibold text-slate-200 flex items-center gap-1.5">
                          <BarChart3 className="h-3.5 w-3.5 text-indigo-400" />
                          精确 Shapley 贡献度归因（2³ = {shapleyAttribution.coalition_count} 联盟）
                        </div>
                        <div className="text-[11px] font-mono text-slate-400">
                          ∑φᵢ = {shapleyAttribution.efficiency_sum.toFixed(4)} | v(N)-v(∅) ={' '}
                          {(
                            shapleyAttribution.grand_coalition_value -
                            shapleyAttribution.empty_coalition_value
                          ).toFixed(4)}
                        </div>
                      </div>

                      <div className="space-y-2.5">
                        {shapleyAttribution.attributions.map((attr) => {
                          const pct = Math.max(0, Math.min(100, attr.normalized_share * 100));
                          return (
                            <div
                              key={attr.agent_id}
                              data-testid={`shapley-bar-${attr.agent_id}`}
                              className="space-y-1"
                            >
                              <div className="flex items-center justify-between text-xs">
                                <span className="font-mono font-medium text-slate-200">
                                  {attr.agent_id}
                                  {!attr.supports_target && (
                                    <span className="ml-2 text-[10px] px-1.5 py-0.5 rounded bg-slate-800 text-slate-400">
                                      未支持最终根因
                                    </span>
                                  )}
                                </span>
                                <span className="font-mono text-slate-300">
                                  φ = {attr.shapley_value.toFixed(4)} ({pct.toFixed(1)}%)
                                </span>
                              </div>
                              <div className="h-2 w-full rounded-full bg-slate-800 overflow-hidden">
                                <div
                                  className="h-full rounded-full bg-gradient-to-r from-indigo-500 to-cyan-400 transition-all duration-300"
                                  style={{ width: `${pct}%` }}
                                />
                              </div>
                            </div>
                          );
                        })}
                      </div>
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
                          {typeof ev.payload?.evidence_source === 'string' && (
                            <span
                              className={`px-1.5 py-0.5 rounded text-[10px] font-semibold border ${
                                ev.payload.evidence_source === 'LIVE'
                                  ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                                  : 'bg-slate-800 text-amber-200 border-amber-500/30'
                              }`}
                            >
                              {ev.payload.evidence_source === 'LIVE' ? 'LIVE 真实证据' : 'MOCK 合成数据'}
                            </span>
                          )}
                          {typeof ev.payload?.execution_outcome === 'string' &&
                            OUTCOME_BADGE_STYLES[ev.payload.execution_outcome] && (
                              <span
                                className={`px-1.5 py-0.5 rounded text-[10px] font-semibold border ${
                                  OUTCOME_BADGE_STYLES[ev.payload.execution_outcome].cls
                                }`}
                              >
                                {OUTCOME_BADGE_STYLES[ev.payload.execution_outcome].label}
                              </span>
                            )}
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
                    <div className="flex flex-wrap items-center justify-between gap-2 mb-1.5">
                      <span className="text-xs font-medium text-slate-400">
                        PG16 共享黑板只读诊断证据
                      </span>
                      <div className="flex flex-wrap gap-1.5">
                        {Object.entries(evidence).map(([k, val]) => {
                          const src =
                            val && typeof val === 'object' && 'evidence_source' in val
                              ? String((val as Record<string, unknown>).evidence_source)
                              : 'MOCK';
                          return (
                            <span
                              key={k}
                              data-testid={`evidence-source-badge-${k}`}
                              className={`px-2 py-0.5 rounded-full text-[10px] font-semibold border ${
                                src === 'LIVE'
                                  ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40'
                                  : 'bg-slate-800 text-amber-200 border-amber-500/40'
                              }`}
                            >
                              {k}: {src === 'LIVE' ? 'LIVE 真实证据' : 'MOCK 合成数据'}
                            </span>
                          );
                        })}
                      </div>
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
