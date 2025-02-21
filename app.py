from flask import Flask, request, json, jsonify, abort
from riskScore import generateRiskScore, getPeersForCompany
from werkzeug import exceptions
from werkzeug.exceptions import HTTPException
from dotenv import load_dotenv

load_dotenv()

debug = True  # global variable setting the debug config


class MissingParam(HTTPException):
    code = 1001
    message = "Missing required fields!"
    publicMessage = "Missing required data to generate Inxeption Score."


class GenerateScoreFailure(HTTPException):
    code = 1002
    message = "Failed generating Inxeption Score."
    publicMessage = "Failed generating Inxeption Score."


class NullValue(HTTPException):
    code = 1003
    message = "Required field value is NULL!"
    publicMessage = (
        "Insufficient data from the data source to generate Inxeption score."
    )


class GetPeersFailure(HTTPException):
    code = 1004
    message = "Failed to identify the Peers"
    publicMessage = "Failed to identify the Peers"


def create_app():
    app = Flask(__name__)

    @app.route("/")
    def helloWorld():
        return "<p>Hello, World!</p>"

    @app.errorhandler(404)
    def page_not_found(e):
        return "<h1>404</h1><p>The resource could not be found.</p>", 404

    def handle_error(e):
        return jsonify(
            {
                "error": {
                    "code": e.code,
                    "message": e.message,
                    "publicMessage": e.publicMessage,
                }
            }
        )

    @app.errorhandler(Exception)
    def handle_exception(e):
        if isinstance(e, HTTPException):
            return e

        res = {
            "error": {
                "code": 500,
                "message": "Failed generating Inxeption score",
                "publicMessage": "Failed generating Inxeption score",
            }
        }
        if debug:
            res["error"]["message"] = e.message if hasattr(e, "message") else f"{e}"

        return jsonify(res), 500

    app.register_error_handler(MissingParam, handle_error)
    app.register_error_handler(GenerateScoreFailure, handle_error)
    app.register_error_handler(GetPeersFailure, handle_error)
    app.register_error_handler(NullValue, handle_error)

    @app.route("/getRiskScore", methods=["POST"])
    def getRiskScore():
        data = json.loads(request.data)
        # data['peerScore'] = True
        if data:
            if (
                "experianBizJson" not in data.keys()
                or "assessment" not in data.keys()
                or data["experianBizJson"] is None
                or data["assessment"] is None
            ):
                raise MissingParam()

            if "businessFacts" in data["experianBizJson"]:
                ## raise if any of feature is miissing
                if (
                    not "salesRevenue" in data["experianBizJson"]["businessFacts"]
                    or not "daysBeyondPayments" in data["assessment"]
                    or not "owedAmount" in data["assessment"]
                    or not "highestCreditAmount" in data["assessment"]
                    or not "paydexScore" in data["assessment"]
                    or not "maximumRecommendedCredit" in data["assessment"]
                    or not "delinquencyRawScore" in data["assessment"]
                    or not "delinquencyScoreClass" in data["assessment"]
                ):
                    raise MissingParam()
                ## raise if all of features are NULL
                if (
                    (
                        not data["experianBizJson"]["businessFacts"]["salesRevenue"]
                        or data["experianBizJson"]["businessFacts"]["salesRevenue"]
                        is None
                    )
                    and (
                        not data["assessment"]["daysBeyondPayments"]
                        or data["assessment"]["daysBeyondPayments"] is None
                    )
                    and (
                        not data["assessment"]["owedAmount"]
                        or data["assessment"]["owedAmount"] is None
                    )
                    and (
                        not data["assessment"]["highestCreditAmount"]
                        or data["assessment"]["highestCreditAmount"] is None
                    )
                    and (
                        not data["assessment"]["paydexScore"]
                        or data["assessment"]["paydexScore"] is None
                    )
                    and (
                        not data["assessment"]["maximumRecommendedCredit"]
                        or data["assessment"]["maximumRecommendedCredit"] is None
                    )
                    and (
                        not data["assessment"]["delinquencyRawScore"]
                        or data["assessment"]["delinquencyRawScore"] is None
                    )
                    and (
                        not data["assessment"]["delinquencyScoreClass"]
                        or data["assessment"]["delinquencyScoreClass"] is None
                    )
                ):
                    raise NullValue()
            else:
                raise MissingParam()
            try:
                print("why I am generating")
                return generateRiskScore(data)
            except:
                raise GenerateScoreFailure()
        else:
            raise MissingParam()

    @app.route("/getPeers", methods=["POST"])
    def getPeers():
        data = json.loads(request.data)
        if data:
            try:
                return getPeersForCompany(data)
            except:
                raise GetPeersFailure()
        else:
            raise MissingParam()

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=4046)

