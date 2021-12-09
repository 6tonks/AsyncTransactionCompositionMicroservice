from __future__ import annotations
from typing import List, Protocol, Tuple
from flask import Flask, request, Response
from flask_cors import CORS

import json
import logging
import requests
from dataclasses import dataclass

import time

import asyncio
import aiohttp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)

money_api_path = "https://nz2mapdki5.execute-api.us-east-1.amazonaws.com/beta/"
portfolio_api_path = "https://sldnph4bq3.execute-api.us-east-1.amazonaws.com/beta/"

class Task(Protocol):
    async def do_async(self, *args, **kwargs) -> bool:
        ...

    async def undo_async(self, *args, **kwargs) -> bool:
        ...

    @property
    def name(self) -> str:
        ...

    async def do(self) -> bool:
        ...

    async def undo(self) -> bool:
        ...

@dataclass
class AdjustMoneyManagement:
    user_id: str
    amount: int

    @property
    def name(self):
        return "Money Management"

    async def do(self):
        user_money_path = f"{money_api_path}money/{self.user_id}"
        method = "addition" if self.amount >= 0 else "deduction"
        body = {"method": method, "money_amount": str(abs(self.amount))}

        return requests.put(user_money_path, json=body).ok

    async def undo(self):
        user_money_path = f"{money_api_path}money/{self.user_id}"
        method = "addition" if self.amount <= 0 else "deduction"
        body = {"method": method, "money_amount": str(abs(self.amount))}

        return requests.put(user_money_path, json=body).ok

    async def do_async(self, session):
        user_money_path = f"{money_api_path}money/{self.user_id}"
        method = "addition" if self.amount >= 0 else "deduction"
        body = {"method": method, "money_amount": str(abs(self.amount))}

        async with session.put(user_money_path, json=body) as resp:
            return resp.ok

    async def undo_async(self, session):
        user_money_path = f"{money_api_path}money/{self.user_id}"
        method = "addition" if self.amount <= 0 else "deduction"
        body = {"method": method, "money_amount": str(abs(self.amount))}

        async with session.put(user_money_path, json=body) as resp:
            return resp.ok


@dataclass
class AdjustInvestmentPortfolio:
    user_id: str
    ticker: str
    quantity: int

    @property
    def name(self):
        return "Investment Portfolio"

    async def do(self):
        action = "sell" if self.quantity >= 0 else "buy"
        investment_portfolio_path = f"{portfolio_api_path}/api/{action}/{self.user_id}"
        body = {
            "user_id": self.user_id,
            "ticker": self.ticker,
            "quantity": self.quantity,
        }
        return requests.post(investment_portfolio_path, json=body).ok

    async def undo(self):
        action = "buy" if self.quantity >= 0 else "sell"
        investment_portfolio_path = f"{portfolio_api_path}/api/{action}/{self.user_id}"
        body = {
            "user_id": self.user_id,
            "ticker": self.ticker,
            "quantity": self.quantity,
        }
        return requests.post(investment_portfolio_path, json=body).ok

    async def do_async(self, session):
        action = "sell" if self.quantity >= 0 else "buy"
        investment_portfolio_path = f"{portfolio_api_path}/api/{action}/{self.user_id}"
        body = {
            "user_id": self.user_id,
            "ticker": self.ticker,
            "quantity": self.quantity,
        }

        async with session.post(investment_portfolio_path, json=body) as resp:
            return resp.ok

    async def undo_async(self, session):
        action = "buy" if self.quantity >= 0 else "sell"
        investment_portfolio_path = f"{portfolio_api_path}/api/{action}/{self.user_id}"
        body = {
            "user_id": self.user_id,
            "ticker": self.ticker,
            "quantity": self.quantity,
        }

        async with session.post(investment_portfolio_path, json=body) as resp:
            return resp.ok

Pipeline = List[Task]
async def execute_pipeline_async(pipeline: Pipeline) -> Tuple[int, List[str]]:
    """Returns success_code, failed tasks"""
    async with aiohttp.ClientSession() as session:
        responses = await asyncio.gather(
            *[asyncio.ensure_future(task.do_async(session)) for task in pipeline]
        )
        if all(resp for resp in responses):
            return 1, []
        await asyncio.gather(
            *[
                asyncio.ensure_future(task.undo_async(session))
                for task, resp in zip(pipeline, responses)
                if resp
            ]
        )
        failed_pipes = [pipe.name for pipe, resp in zip(pipeline, responses) if not resp]
        return 0, failed_pipes


async def execute_pipeline(pipeline: Pipeline) -> Tuple[int, List[str]]:
    """Returns success_code, failed tasks"""
    responses = await asyncio.gather(*[task.do() for task in pipeline])
    if all(resp for resp in responses):
        return 1, []
    await asyncio.gather(
        *[task.undo() for task, resp in zip(pipeline, responses) if resp]
    )
    failed_pipes = [task.name for task, resp in zip(pipeline, responses) if not resp]
    return 0, failed_pipes

@app.route("/", methods=["GET"])
def index():
    return Response(
        json.dumps({
            "message": "Welcome to the async transationc microservice",
            "links": [
                {
                    'rel': 'self',
                    'href': '/'
                },
                {
                    'rel': 'stock_transaction',
                    'href': '/stockTransaction'
                }
            ]
        }),
        status=200,
        content_type="application/json",
    )

@app.route("/stockTransaction", methods=["POST"])
def transaction():
    #start = time.perf_counter()
    input = request.get_json()
    if request.method == "POST":
        # Gets stock's price and money, verifies transaction is feasible

        pipeline = []
        if input["transaction_type"] == "BUY":
            pipeline = [
                AdjustMoneyManagement(
                    input["user_id"], -input["price"] * input["quantity"]
                ),
                AdjustInvestmentPortfolio(
                    input["user_id"], input["ticker"], input["quantity"]
                ),
            ]
        elif input["transaction_type"] == "SELL":
            pipeline = [
                AdjustMoneyManagement(
                    input["user_id"], input["price"] * input["quantity"]
                ),
                AdjustInvestmentPortfolio(
                    input["user_id"], input["ticker"], -input["quantity"]
                ),
            ]
        res = asyncio.run(execute_pipeline_async(pipeline))

        #print("Executed in time ", time.perf_counter() - start)
        if res[0]:
            return Response(
                json.dumps({"success": 1}), status=201, content_type="application/json"
            )
        return Response(
            json.dumps({"error": f"{', '.join(res[1])} failed"}),
            status=201,
            content_type="application/json",
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
