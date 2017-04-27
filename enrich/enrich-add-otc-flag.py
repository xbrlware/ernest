import argparse
from modules.add_otc_flags import ADD_OTC_FLAGS
from generic.logger import LOGGER


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    parser.add_argument("--lookup-path",
                        type=str,
                        action='store',
                        default='../reference/sic_ref.p')
    parser.add_argument("--index",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument("--field-name",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    parser.add_argument('--expected',
                        type=str,
                        dest='expected',
                        action="store")
    parser.add_argument('--date',
                        type=str,
                        dest='date',
                        action="store")

    args = parser.parse_args()
    logger = LOGGER('enrich_add_otc_flag', args.log_file).create_parent()
    aof = ADD_OTC_FLAGS(args)
    aof.main()
