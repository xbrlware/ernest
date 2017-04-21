from datetime import date, timedelta


def fye_from_scratch():
    return {
        "query": {
            "filtered": {
                "filter": {
                    "exists": {
                        "field": "_enrich.period"
                    }
                }
            }
        }
    }


def fye_most_recent():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "filtered": {
                            "filter": {
                                "exists": {
                                    "field": "_enrich.period"
                                }
                            }
                        }
                    },
                    {
                        "range": {
                            "date": {
                                "gte": str(date.today() - timedelta(days=9))
                            }
                        }
                    }
                ]
            }
        }
    }
