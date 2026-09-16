# GPT-Live-1 port for the Voice PE add-on — scope & plan

*Author: Claw, 2026-09-16 07:25 PT. Status: PROPOSAL — Edward reviews before any code.*

## Goal
Run the office Voice PE (and later the patio) on OpenAI **GPT-Live-1** (full-duplex speech-to-speech,
new `v1/live/sessions` API) instead of `gpt-realtime-2`, keeping HA device control, OpenClaw
memory/escalation and the announce endpoint working.

## What we have (verified 2026-09-16)
| Item | Status |
|---|---|
| OpenAI key access to Live API | ✅ `session.start {model: gpt-live-1, delegation: {type: responses, responses: {model: gpt-5.4-mini}}}` → `session.started` (voice `marin`, PCM 24 kHz) |
| pipecat support | ✅ `pipecat.services.openai.live.OpenAILiveLLMService` in **pipecat-ai ≥ 1.x** (latest 1.10.0), 2 delegation modes |
| Add-on source | ✅ local clone `projects/voicepe-realtime` = upstream HEAD (`5824ea9`); no Live work upstream |
| Add-on pipecat pin | ❌ **0.0.97** (`openai_realtime_voice_agent/pyproject.toml`); Live module absent there |
| Python | ✅ add-on `python = ">=3.11,<3.14"`, Debian bookworm base — fine for pipecat 1.10 |
| Audio | ✅ pipeline already runs at 24 kHz (`PIPELINE_SAMPLE_RATE = 24000`) with input resampler; Live outputs PCM 24 kHz — no change |
| HA control | via HA MCP Server tools already registered in the add-on; in Live they run as **function tools executed client-side** under `ResponsesDelegation` |
| Dev environment | Orca worktree (Orca ADE rule) on a fork `myzona/voicepe-realtime`; add-on built by HA Supervisor on VM 140 from a local add-on repo or the fork |

## The real work = pipecat 0.0.97 → 1.10 migration
The add-on is ~7.3k lines of Python built on pipecat 0.0.97 (`OpenAIRealtimeLLMService`,
`WebsocketServerTransport`, custom processors, MCP client). pipecat 1.0 changed public APIs
(settings objects, `LLMContext`, turn strategies, worker runner). This migration is the bulk and the
risk; the Live service itself is a bounded addition once the pipeline runs on 1.x.

## Phases
1. **Baseline on pipecat 1.10 (Realtime unchanged)** — fork, bump pin, fix imports/API changes,
   build add-on locally on VM 140 as `local_openai_realtime_voice_agent`, regression on office PE:
   wake word, HA tool call (light on/off), recall_memory via bridge, announce endpoint, timers.
   *Exit: identical behaviour on gpt-realtime-2.* (est. 0.5–1 day)
2. **Add Live-1 path** — new option `openai_model: gpt-live-1` selects `OpenAILiveLLMService` with
   `ResponsesDelegation(model = new option live_backend_model, default gpt-5.4-mini)`; register the
   same tool handlers (HA MCP, ask_openclaw, recall_memory, disconnect, web_search); persona
   instructions → `session.instructions`; voice option (Live voices ≠ Realtime voices — `marin`
   default; verify list). Turn handling: `ExternalUserTurnStrategies(enable_interruptions=False)`,
   drop client VAD/turn-detection settings for this mode. (est. 0.5 day)
3. **Wake-word / session lifecycle** — Live sessions are full-duplex and expire (~1 h `expires_at`);
   keep the add-on's wake-gated connect/disconnect and wedge-watchdog semantics; confirm the
   `disconnect` tool still ends the turn. (est. 0.25 day)
4. **Regression + cost check** — same checklist as phase 1 on Live-1; measure $/min
   (Live voice layer ~$0.05/min + backend tokens) against gpt-realtime-2. (est. 0.25 day)
5. **Upstream PR** to TristanBrotherton (optional, Edward's call).

**Total estimate: 1.5–2 days of agent work, plus Edward's ears for regression.**

## Open questions / risks
- pipecat 1.x `[mcp]` extra still provides the MCP client the add-on uses? (verify in phase 1)
- `WebsocketServerTransport` API differences (per-connection transport patch from upstream #3).
- Live voices and instruction limits; `ClientDelegation` (Claw as backend) is a later option, not v1.
- Two speakers of truth for "who is speaking" (speaker ID) — unchanged, runs before the model.
- Security for the patio: same rule as today — patio persona gets a radio-only tool set.

## Decision needed from Edward
Go / no-go on phase 1 (fork + pipecat upgrade). If go: fork `TristanBrotherton/voicepe-realtime` →
`myzona/voicepe-realtime`, Orca worktree `live1-port`, Opencode as dev agent.
