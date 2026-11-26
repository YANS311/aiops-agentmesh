from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class DiagnosticToolMapping(BaseModel):
    log_agent: str
    db_agent: str


class RemediationSpec(BaseModel):
    tool_name: str
    risk_level: str = Field(pattern="^(HIGH|CRITICAL)$")
    requires_human_approval: bool = True
    command_template: str
    blast_radius: str
    rollback_command: str


class RunbookSkill(BaseModel):
    skill_id: str
    fault_type: str
    title: str
    description: str
    default_severity: str
    default_service: str
    diagnostic_tools: DiagnosticToolMapping
    remediation: RemediationSpec


class SkillRegistry:
    """In-memory registry of declarative YAML Runbook Skills."""

    def __init__(self, skills_dir: Path) -> None:
        self.skills_dir = skills_dir
        self._by_fault_type: dict[str, RunbookSkill] = {}
        self._by_skill_id: dict[str, RunbookSkill] = {}
        self.reload()

    def reload(self) -> None:
        self._by_fault_type.clear()
        self._by_skill_id.clear()
        if not self.skills_dir.exists():
            raise FileNotFoundError(f"Skills directory does not exist: {self.skills_dir}")

        for yaml_path in sorted(self.skills_dir.glob("*.yaml")):
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            skill = RunbookSkill.model_validate(raw)
            self._by_fault_type[skill.fault_type] = skill
            self._by_skill_id[skill.skill_id] = skill

    def list_skills(self) -> list[RunbookSkill]:
        return list(self._by_skill_id.values())

    def match_for_fault(self, fault_type: str) -> RunbookSkill | None:
        return self._by_fault_type.get(fault_type)

    def get_by_id(self, skill_id: str) -> RunbookSkill | None:
        return self._by_skill_id.get(skill_id)
