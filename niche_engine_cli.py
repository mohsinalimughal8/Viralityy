"""
Viralityy — M4a: Niche Engine CLI wrapper
------------------------------------------
Called by niche_engine_bridge.js (Node → Python subprocess).
Always outputs clean JSON to stdout — never print anything else here.

Commands:
  python niche_engine_cli.py recommend --plan combo_pro --count 10
  python niche_engine_cli.py detail --niche psychology_facts
  python niche_engine_cli.py categories
  python niche_engine_cli.py compare --niches psychology_facts,ai_tools,book_summaries
"""

import sys
import json
import argparse
from niche_engine import NicheEngine

engine = NicheEngine()

def main():
    parser = argparse.ArgumentParser(add_help=False)
    subparsers = parser.add_subparsers(dest='command')

    # recommend
    rec = subparsers.add_parser('recommend')
    rec.add_argument('--plan', default='combo_pro')
    rec.add_argument('--count', type=int, default=10)
    rec.add_argument('--category', default=None)

    # detail
    det = subparsers.add_parser('detail')
    det.add_argument('--niche', required=True)

    # categories
    subparsers.add_parser('categories')

    # compare
    cmp = subparsers.add_parser('compare')
    cmp.add_argument('--niches', required=True)  # comma-separated

    args = parser.parse_args()

    if args.command == 'recommend':
        result = engine.recommend(plan=args.plan, count=args.count, category=args.category)
    elif args.command == 'detail':
        result = engine.get_niche_detail(args.niche)
    elif args.command == 'categories':
        result = engine.get_categories()
    elif args.command == 'compare':
        niche_ids = [n.strip() for n in args.niches.split(',')]
        result = engine.compare(niche_ids)
    else:
        result = {"error": "Unknown command"}

    print(json.dumps(result))

if __name__ == '__main__':
    main()
