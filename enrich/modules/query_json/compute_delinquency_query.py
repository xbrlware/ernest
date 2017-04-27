def compute_delinquency_query():
    return {
        "query": {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "_enrich.status.cat":
                            ["Large Accelerated Filer",
                             "Accelerated Filer",
                             "Smaller Reporting Company",
                             "Non-accelerated Filer"
                             ]
                        }
                    },
                    {
                        "query": {
                            "filtered": {
                                "filter": {
                                    "exists": {
                                        "field": "_enrich.period"
                                    }
                                }
                            }
                        }
                    },
                    {
                        "query": {
                            "filtered": {
                                "filter": {
                                    "missing": {
                                        "field": "_enrich.deadline"
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
