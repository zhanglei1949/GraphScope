from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *


def callProcedureWithHttpCurrent(sess: Session, name: str):
    req = QueryRequest(query_name=name, arguments=[])
    resp = sess.call_procedure_current(params=req)
    print(resp)
    assert resp.is_ok()
    print("call procedure result: ", resp.get_value())


if __name__ == "__main__":
    # parse command line args
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", type=str, default="http://localhost:7777")
    parser.add_argument("--proc-name", type=str, default="huoyan")

    # finish
    args = parser.parse_args()
    print(args)

    print("connecting to ", args.endpoint)
    driver = Driver(endpoint=args.endpoint)
    sess = driver.session()

    callProcedureWithHttpCurrent(sess, args.proc_name)

