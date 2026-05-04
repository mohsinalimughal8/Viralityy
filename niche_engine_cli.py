"""
Viralityy — Niche Engine CLI wrapper (v2)
-----------------------------------------
Called by niche_engine_v2_bridge.js (Node → Python subprocess).
Always outputs clean JSON to stdout.

Commands:
  python niche_engine_cli.py recommend --plan combo_pro --count 10
  python niche_engine_cli.py detail    --niche psychology_facts
  python niche_engine_cli.py categories
  python niche_engine_cli.py compare   --niches psychology_facts,ai_tools
  python niche_engine_cli.py analyse   --keyword "stoicism" --plan combo_pro
  python niche_engine_cli.py batch     --keywords "stoicism,AI tools" --plan combo_pro
  python niche_engine_cli.py discover  --plan combo_pro --top 10 --min-score 60
  python niche_engine_cli.py search    --keyword "budget travel" --plan combo_pro
"""

import sys
import json
import argparse
import asyncio
from niche_engine_v2 import NicheEngine

engine = NicheEngine()

def main():
    parser = argparse.ArgumentParser(add_help=False)
    sub    = parser.add_subparsers(dest='command')

    # Layer 1 — offline (backwards-compatible)
    rec = sub.add_parser('recommend')
    rec.add_argument('--plan', default='combo_pro')
    rec.add_argument('--count', type=int, default=10)
    rec.add_argument('--category', default=None)

    det = sub.add_parser('detail')
    det.add_argument('--niche', required=True)

    sub.add_parser('categories')

    cmp = sub.add_parser('compare')
    cmp.add_argument('--niches', required=True)

    # Layer 1+2 — live YouTube analysis
    ana = sub.add_parser('analyse')
    ana.add_argument('--keyword', required=True)
    ana.add_argument('--plan', default='combo_pro')

    bat = sub.add_parser('batch')
    bat.add_argument('--keywords', required=True)
    bat.add_argument('--plan', default='combo_pro')

    dis = sub.add_parser('discover')
    dis.add_argument('--plan', default='combo_pro')
    dis.add_argument('--top', type=int, default=10)
    dis.add_argument('--min-score', type=float, default=60.0)
    dis.add_argument('--force', action='store_true')

    srch = sub.add_parser('search')
    srch.add_argument('--keyword', required=True)
    srch.add_argument('--plan', default='combo_pro')

    args = parser.parse_args()

    if args.command == 'recommend':
        result = engine.recommend(plan=args.plan, count=args.count, category=args.category)
    elif args.command == 'detail':
        result = engine.get_niche_detail(args.niche)
    elif args.command == 'categories':
        result = engine.get_categories()
    elif args.command == 'compare':
        result = engine.compare([n.strip() for n in args.niches.split(',')])
    elif args.command == 'analyse':
        result = asyncio.run(engine.analyse(args.keyword, args.plan))
    elif args.command == 'batch':
        keywords = [k.strip() for k in args.keywords.split(',')]
        result   = asyncio.run(engine.analyse_batch(keywords, args.plan))
    elif args.command == 'discover':
        result = asyncio.run(engine.discover(
            plan=args.plan, top_n=args.top,
            min_score=args.min_score, force_refresh=args.force,
        ))
    elif args.command == 'search':
        result = asyncio.run(engine.search(args.keyword, args.plan))
    else:
        result = {"error": "Unknown command"}

    print(json.dumps(result, default=str))

if __name__ == '__main__':
    main()
