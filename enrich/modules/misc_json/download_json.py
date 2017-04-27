def download_json():
    return {
        "10-K": {
            "Large Accelerated Filer": {
                "preDec1506": 75,
                "postDec1506": 60
            },
            "Accelerated Filer": {
                "preDec1503": 90,
                "postDec1503": 75
            },
            "Non-accelerated Filer": 90,
            "Smaller Reporting Company": 90
        },
        "10-Q": {
            "Large Accelerated Filer": 40,
            "Accelerated Filer": {
                "preDec1504": 45,
                "postDec1504": 40
            },
            "Non-accelerated Filer": 45,
            "Smaller Reporting Company": 45
        }
    }
