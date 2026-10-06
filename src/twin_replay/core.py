from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json


@dataclass(frozen=True)
class Telemetry:
    identifier: str
    version: str
    status: str
    owner: str
    payload: dict[str, str]
    created_at: str

    @classmethod
    def create(cls, identifier: str, version: str, status: str, owner: str, payload: dict[str, str]):
        if not identifier.strip():
            raise ValueError("标识不能为空")
        if not version.strip():
            raise ValueError("版本不能为空")
        if status not in {"draft", "active", "closed"}:
            raise ValueError("状态不受支持")
        normalized = {str(k): str(v) for k, v in sorted(payload.items())}
        stamp = datetime.now(timezone.utc).isoformat()
        return cls(identifier.strip(), version.strip(), status, owner.strip(), normalized, stamp)

    def as_dict(self) -> dict[str, object]:
        return {
            "identifier": self.identifier,
            "version": self.version,
            "status": self.status,
            "owner": self.owner,
            "payload": dict(self.payload),
            "created_at": self.created_at,
        }


def summarize(item: Telemetry) -> str:
    body = json.dumps(item.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    return f"{item.identifier}@{item.version}|{item.status}|{digest}"

