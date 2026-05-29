# Research Intelligence System - MVP

Sistema multiagente simple para investigación automática y generación de reportes.

## 🎯 Objetivo

Demostrar un sistema de IA donde múltiples agentes colaboran en un workflow lineal para:
1. Investigar un tema
2. Estructurar hallazgos
3. Generar reportes

## 📦 Estructura del Proyecto

```
research_ai_system/
├── app/
│   ├── __init__.py
│   ├── models.py              # Contratos (Task, Result, Event, Context)
│   ├── base_agent.py          # Clase abstracta de agentes
│   ├── orchestrator.py        # Coordinador principal
│   └── event_bus.py           # Sistema de eventos simple
│
├── agents/
│   ├── __init__.py
│   ├── research_agent.py      # Agente de investigación
│   └── planner_agent.py       # Agente de estructuración
│
├── main.py                     # Punto de entrada
├── requirements.txt            # Dependencias
└── README.md                   # Este archivo
```

## 🚀 Inicio Rápido

### 1. Instalación

```bash
pip install -r requirements.txt
```

### 2. Ejecutar el Sistema

```bash
python main.py
```

Esto iniciará un workflow completo que:
- Crea dos tareas (research + planning)
- Registra dos agentes
- Ejecuta el workflow de forma lineal
- Genera un reporte en Markdown
- Guarda resultados en JSON

### 3. Salida

El sistema genera:
- `research_report.md` - Reporte formateado
- `workflow_result.json` - Datos completos en JSON
- `system.log` - Logs estructurados

## 🧩 Componentes Clave

### Models (contratos.py)

```python
Task              # Unidad de trabajo
Result            # Resultado de ejecución
Event             # Evento del sistema
Context           # Contexto compartido
AgentState        # Estado de agente
ToolResult        # Resultado de herramienta
```

### BaseAgent

```python
class BaseAgent(ABC):
    async def execute(task: Task, context: Context) -> Result
    async def _execute_impl(task: Task, context: Context) -> Any  # Override
    def get_capabilities() -> List[str]
    def emit_event(event_type: str, payload: Dict)
    def use_tool(tool_name: str, **kwargs) -> ToolResult
```

### Orchestrator

```python
orchestrator = Orchestrator()
orchestrator.register_agent(agent)
result = await orchestrator.execute_task(task, agent_id)
results = await orchestrator.execute_workflow([(task1, agent1), (task2, agent2)])
```

### EventBus

```python
event_bus = EventBus()
event_bus.subscribe("task.completed", handler)
await event_bus.emit(event)
```

## 🧠 Flujo de Ejecución

```
Usuario: "Investiga tendencias IA 2026"
    ↓
[Orchestrator] Recibe solicitud
    ↓
[ResearchAgent] Investiga tema
    ├─ Buscaría en web (TODO)
    ├─ Sintetizaría hallazgos (TODO)
    └─ Retorna: {"findings": [...], "confidence": 0.82}
    ↓
[Contexto] Se actualiza con hallazgos
    ↓
[PlannerAgent] Estructura reporte
    ├─ Organiza datos
    ├─ Genera insights
    └─ Retorna: {"title": "...", "sections": [...]}
    ↓
[Orchestrator] Retorna resultado final
    ↓
Usuario recibe: Reporte estructurado
```

## 🎯 Filosofía de Diseño (MVP)

### ✅ Lo que SÍ tiene el MVP

- Flujo lineal determinístico (sin loops autónomos)
- 2 agentes especializados
- Contexto compartido simple
- Event logging básico
- Contratos versionables
- Ejecución async

### ❌ Lo que NO tiene el MVP (para después)

- Persistencia en BD
- Vector embeddings
- Semantic memory
- Distributed execution
- Event replay
- Multi-provider LLM
- Capability discovery dinámico
- Autonomy loops
- Self-evaluation

## 🛠️ Extensiones Futuras

### Fase 2: Herramientas Reales
- WebSearchTool (integración real)
- LLMTool (OpenAI/Anthropic)
- FileWriterTool

### Fase 3: Más Agentes
- CodingAgent
- AnalysisAgent
- ExecutorAgent

### Fase 4: Persistencia
- PostgreSQL storage
- Redis cache
- Vector DB

### Fase 5: Observabilidad
- Distributed tracing
- Metrics
- Dashboard

## 📊 Métricas de Éxito

El MVP es exitoso si:

- [x] Flujo completo funciona end-to-end
- [x] Agentes se comunican correctamente
- [ ] Contexto se propaga sin problemas (necesita test)
- [ ] Output es útil y estructurado
- [ ] Sistema es debuggeable
- [ ] Costo es controlado
- [ ] Sin loops infinitos

## 🔧 Testing

```bash
python -m pytest tests/
```

## 📝 Logging

El sistema genera logs estructurados en:
- Console (INFO+)
- `system.log` (DEBUG+)

Buscar eventos específicos:
```python
from app import EventBus

bus = EventBus()
task_events = bus.get_events("task.completed")
```

## 🚫 Errores Comunes

### "Agente no encontrado"
```python
# ❌ Incorrecto
await orchestrator.execute_task(task, "unknown_agent")

# ✅ Correcto
orchestrator.register_agent(my_agent)
await orchestrator.execute_task(task, my_agent.agent_id)
```

### "No hay hallazgos"
El PlannerAgent requiere que ResearchAgent ejecute primero:
```python
# ✅ Correcto
await orchestrator.execute_workflow([
    (research_task, "research_agent"),   # Primero
    (planning_task, "planner_agent"),    # Segundo
])
```

## 📚 Recursos

- Documento de arquitectura: `ARCHITECTURE.md` (próximamente)
- Guide de desarrollo: `DEVELOPMENT.md` (próximamente)

## 👥 Autores

Sistema desarrollado con metodología de arquitectura senior:
- Énfasis en contratos antes de código
- MVP mínimo antes de framework
- Validación de utilidad antes de complejidad

---

**Estado:** MVP Funcional v0.1
**Última actualización:** 2026-05-19
