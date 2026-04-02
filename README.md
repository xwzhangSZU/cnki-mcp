# cnki-mcp

MCP server for searching academic papers on [CNKI](https://www.cnki.net/) (China National Knowledge Infrastructure). Built with [FastMCP](https://github.com/jlowin/fastmcp) and [Playwright](https://playwright.dev/). Supports journal filtering via CNKI professional search.

## Install

```bash
# uv (recommended)
uv tool install "cnki-mcp @ git+https://github.com/xwzhangSZU/cnki-mcp.git"

# or pip
pip install git+https://github.com/xwzhangSZU/cnki-mcp.git
```

## Configure

**Claude Code** — add to `.mcp.json` or run `claude mcp add cnki cnki-mcp`:

```json
{
  "mcpServers": {
    "cnki": { "command": "cnki-mcp" }
  }
}
```

**Cursor** — add to `~/.cursor/mcp.json` with the same format.

## Prerequisites

Chrome 必须开启远程调试，MCP 通过 CDP 连接你的日常浏览器（带 cookies + 机构下载权限）。

MCP 会自动从 `DevToolsActivePort` 文件发现调试端口，无需指定固定端口号。只要 Chrome 以调试模式运行，MCP 就能自动连上。

如果 CDP 连接失败，MCP 会报错并提示原因，不会静默回退到 headless 模式（headless 无法使用机构下载权限）。

## Tools

| Tool | Description |
|------|-------------|
| `search_cnki` | Search papers by keyword, author, title, DOI, etc. Optional `journal` param restricts to a specific journal via CNKI professional search. |
| `get_paper_detail` | Get full metadata (abstract, authors, keywords, DOI, citations, ...) for a paper. |
| `get_paper_bibtex` | Get BibTeX citation from CNKI's official export. |
| `download_paper_pdf` | Download a paper's PDF to a local directory. Requires institutional access. |
| `find_best_match` | Find the paper whose title best matches the input. |

## Examples

- "Search CNKI for papers about 人民币国际化"
- "Search CNKI for 人民币国际化 papers in 经济研究"
- "Search CNKI for 人民币国际化 papers in 管理世界"
- "Get details for this CNKI paper: \<url\>"
- "Download the PDF of this paper to ~/papers: \<url\>"

## Requirements

- Python >= 3.10
- Chrome/Chromium with remote debugging enabled

## License

MIT
