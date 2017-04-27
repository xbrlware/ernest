def aqfs_fye_query():
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
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": "sub"
                                }
                            }
                        }
                    },
                    {
                        "filtered": {
                            "filter": {
                                "exists": {
                                    "field": "__meta__.financials"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }


def aqfs_fye_query_func():
    return {
        "query": {
            "bool": {
                "must_not": [
                    {
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": "_enrich.period"
                                }
                            }
                        }
                    },
                    {
                        "match": {
                            "sub.matched": "acc_no"
                        }
                    },
                    {
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": "__meta__.financials"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }


def add_id_query(cik, period):
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
                        "range": {
                            "max_date": {
                                "gte": period
                            }
                        }
                    },
                    {
                        "range": {
                            "min_date": {
                                "lte": period
                            }
                        }
                    }
                ]
            }
        }
    }


def add_id_sub_query(acc):
    return {
        "query": {
            "bool": {
                "must_not": [
                    {
                        "match": {
                            "fy.cat": ""
                        }
                    }
                ],
                "must": [
                    {
                        "match": {
                            "adsh.cat": acc
                        }
                    }
                ]
            }
        }
    }
