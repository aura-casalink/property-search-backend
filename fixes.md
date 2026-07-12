# fixes.md — registro de cambios mergeados (append-only)

Reglas (protocolo común AURA — ver CLAUDE.md):
- **SOLO añadir al final.** Nunca reescribir, reformatear ni renumerar entradas previas.
- Una entrada por merge: **qué se ataca, cómo, y cómo queda**, más riesgos/deuda.
- Para relacionar con una entrada previa, referénciala desde la nueva ("supersedes Fix N").
- El número de Fix se re-verifica JUSTO antes de commitear (otra sesión puede habérselo llevado).
- Tras añadir: verificar que el número de líneas del fichero creció y mostrar el tail a Pablo.

---

## Fix 1 — 2026-07-13 — Adopción del protocolo común AURA
- **Qué se ataca:** este repo no tenía instrucciones para sesiones de Claude Code ni registro
  de cambios entre sesiones/ordenadores.
- **Cómo:** se añade `CLAUDE.md` (instrucciones específicas del repo; el protocolo común vive
  en `aura-contracts/protocol/CLAUDE-master.md`) y este `fixes.md`.
- **Cómo queda:** toda sesión de Claude Code carga `CLAUDE.md` automáticamente y cada merge
  futuro deja una entrada aquí.
- **Riesgos/deuda:** ninguno — solo documentación, sin cambios de código.
