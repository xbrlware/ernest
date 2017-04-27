def build_nt_most_recent():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": "__meta__.migrated"
                                    }
                                }
                            }
                        },
                    {
                        "terms": {
                            "form.cat": ["NT 10-K", "NT 10-Q"]
                            }
                        }
                    ]
                }
            }
        }


def add_nt_most_recent():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "form.cat": ["NT 10-K", "NT 10-Q"]
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {
                                    "filtered": {
                                        "filter": {
                                            "missing": {
                                                "field":
                                                "__meta__.match_attempted"
                                                }
                                            }
                                        }
                                    },
                                {
                                    "match": {
                                        "__meta__.matched": False
                                        }
                                    }
                                ],
                            "minimum_should_match": 1
                            }
                        }
                    ]
                }
            }
        }


def build_nt_from_scratch():
    return {
        "query": {
            "terms": {
                "form.cat": ["NT 10-K", "NT 10-Q"]
            }
        }
    }


def add_nt_from_scratch():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "form.cat": ["NT 10-K", "NT 10-Q"]
                        }
                    },
                    {
                        "filtered": {
                            "filter": {
                                "exists": {
                                    "field": "_enrich.period"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }


def match_nt_query(form, cik, period):
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "form.cat": form.replace("NT ", "")
                            }
                        },
                    {
                        "match": {
                            "cik.cat": cik
                            }
                        },
                    {
                        "match": {
                            "_enrich.period": period
                            }
                        },
                    {
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": "_enrich.NT_exists"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }


def enrich_nt_query():
    return {
        "query": {
            "filtered": {
                "filter": {
                    "missing": {
                        "field": "_enrich.period"
                    }
                }
            }
        }
    }
