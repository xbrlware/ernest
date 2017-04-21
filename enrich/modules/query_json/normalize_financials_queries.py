def nfq_from_scratch():
    return {
        "query": {
            "filtered": {
                "filter": {
                    "exists": {
                        "field": "sub.fpID"
                    }
                }
            }
        }
    }


def nfq_most_recent():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "filtered": {
                            "filter": {
                                "exists": {
                                    "field": "sub.fpID"
                                }
                            }
                        }
                    },
                    {
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": "__meta__.financials.scaled"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }


def normalize_query(cik, pp):
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "cik": cik
                        }
                    },
                    {
                        "match": {
                            "sub.fpID": pp
                        }
                    }
                ]
            }
        }
    }
