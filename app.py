from typing import Protocol
from flask import Flask, request, Response
from flask_cors import CORS

import json
import logging
import requests
from dataclasses import dataclass

import asyncio

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)

money_api_path = "https://nz2mapdki5.execute-api.us-east-1.amazonaws.com/beta/"
portfolio_api_path = "https://sldnph4bq3.execute-api.us-east-1.amazonaws.com/beta/"

#  { “user_id”: “102", “ticker”: “ABC”, “quantity”: 10, “price”: 50.0, “transaction_type”: “BUY” }

class Task(Protocol):
    async def do(self):
        ...
    
    async def undo(self):
        ...

    @property
    def name(self) -> str:
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
        
        return requests.put(user_money_path, json = body)

    async def undo(self):
        user_money_path = f"{money_api_path}money/{self.user_id}"
        method = "addition" if self.amount <= 0 else "deduction"
        body = {"method": method, "money_amount": str(abs(self.amount))}
        
        return requests.put(user_money_path, json = body)

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
        investment_portfolio_path = f'{portfolio_api_path}/api/{action}/{self.user_id}'
        body = {"user_id": self.user_id, "ticker": self.ticker, "quantity": self.quantity}
        return requests.post(investment_portfolio_path, json = body)

    async def undo(self):
        action = "buy" if self.quantity >= 0 else "sell"
        investment_portfolio_path = f'{portfolio_api_path}/api/{action}/{self.user_id}'
        body = {"user_id": self.user_id, "ticker": self.ticker, "quantity": self.quantity}
        return requests.post(investment_portfolio_path, json = body)

async def execute_pipeline(pipeline):
    responses = await asyncio.gather(*[pipe.do() for pipe in pipeline])
    if all(resp.ok for resp in responses):
        return 1, []
    await asyncio.gather(*[pipe.undo() for pipe, resp in zip(pipeline, responses) if resp.ok])
    failed_pipes = [pipe.name for pipe, resp in zip(pipeline, responses) if not resp.ok]
    return 0, failed_pipes


@app.route('/stockTransaction', methods=['POST'])
def transaction():
    if request.method == 'POST':
        # Gets stock's price and money, verifies transaction is feasible

        input = request.get_json()
        pipeline = []
        if input['transaction_type'] == 'BUY':
            pipeline = [
                AdjustMoneyManagement(input['user_id'], -input["price"] * input["quantity"]),
                AdjustInvestmentPortfolio(input["user_id"], input["ticker"], input["quantity"])
            ]
        elif input['transaction_type'] == 'SELL':
            pipeline = [
                AdjustMoneyManagement(input['user_id'], input["price"] * input["quantity"]),
                AdjustInvestmentPortfolio(input["user_id"], input["ticker"], -input["quantity"])
            ]

        res = asyncio.run(execute_pipeline(pipeline))
        if res[0]:
            return Response(json.dumps({"success": 1}), status=201, content_type="application/json")
        return Response(json.dumps({"error": f"{', '.join(res[1])} failed"}), status=201, content_type="application/json")


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)