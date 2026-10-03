import argparse
import asyncio
import html
import json
from pathlib import Path
from .core import Budget, BudgetExceeded, Session, Tool, TransientToolError


async def demo():
    counts = {"search":0,"write":0}
    async def search(query):
        counts["search"] += 1
        await asyncio.sleep(.01)
        return {"query":query,"documents":["installation","configuration"]}
    async def write(text):
        counts["write"] += 1
        return {"stored":text}
    async with Session([Tool("search",search,units=2,read_only=True),
                        Tool("write",write,units=3)],
                       budget=Budget(max_calls=5,max_units=10,max_requests=20)) as session:
        results = await asyncio.gather(*(session.call("search",query="setup") for _ in range(8)))
        await session.call("search",query="setup")
        await session.call("write",text="same")
        await session.call("write",text="same")
        try:
            await session.call("write",text="over budget")
        except BudgetExceeded:
            pass
        report = session.report()
        report["fixture"] = "Synthetic async tools; no model or external API called."
        report["physical_calls"] = counts
        report["equivalent_results"] = all(x == results[0] for x in results)
        report["unoptimized_search_calls_for_same_requests"] = 9
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description="Demonstrate bounded tool execution.")
    sub = parser.add_subparsers(dest="command",required=True)
    d = sub.add_parser("demo")
    d.add_argument("--output",default="outputs/demo.json")
    args = parser.parse_args(argv)
    report = asyncio.run(demo())
    path = Path(args.output)
    path.parent.mkdir(parents=True,exist_ok=True)
    payload = json.dumps(report,indent=2)
    path.write_text(payload+"\n",encoding="utf-8")
    page = """<!doctype html><meta charset="utf-8"><title>Agent Tool Budget</title>
<style>body{font:16px system-ui;max-width:1000px;margin:48px auto;padding:24px;background:#101827;color:#eaf0ff}
h1{color:#ffc78b}td,th{padding:12px;border-bottom:1px solid #354663;text-align:left}
table{width:100%;border-collapse:collapse}small{color:#aab}</style><h1>Agent Tool Budget</h1>"""
    page += "<p>Synthetic async tools. No external API cost.</p>"
    page += f"<p>{report['requests']} requests · {report['executed_calls']} physical attempts · {report['reserved_units']} units reserved</p>"
    page += "<table><tr><th>Event</th><th>Tool</th><th>Detail</th></tr>"
    for event in report["events"]:
        detail = {k:v for k,v in event.items() if k not in {"kind","tool"}}
        page += f"<tr><td>{event['kind']}</td><td>{html.escape(event['tool'])}</td><td>{html.escape(json.dumps(detail))}</td></tr>"
    path.with_suffix(".html").write_text(page+"</table>",encoding="utf-8")
    print(json.dumps({k:v for k,v in report.items() if k != "events"},indent=2))
    return 0
