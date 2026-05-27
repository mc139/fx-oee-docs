# Architecture

Three views of the event-driven order system. Each in its own file — open one to get a single-diagram preview in IntelliJ / VS Code.

| # | View                                          | File                                          |
|---|-----------------------------------------------|-----------------------------------------------|
| 1 | High-level architecture                       | [01-high-level.md](./01-high-level.md)        |
| 2 | Order flow — happy path (sequence)            | [02-order-flow.md](./02-order-flow.md)        |
| 3 | Kafka topics — producers / topics / consumers | [03-kafka-topics.md](./03-kafka-topics.md)    |

## Viewing in IntelliJ

- Install the **Mermaid** plugin (Settings → Plugins → search "Mermaid").
- Open any of the files above and switch to the preview pane (right side).
- Plugin renders the embedded ` ```mermaid ` block as an interactive diagram.

VS Code: the built-in Markdown preview plus **Markdown Preview Mermaid Support** extension does the same.

Online fallback: paste the diagram block into <https://mermaid.live>.
