# -*- coding: utf8 -*-
# Author: AcidGo
# Usage: 对于 drds 逻辑实例，导出该实例的 sequences sql 文本。
#   -H|--host <str>: 连接 drds 逻辑库的连接串或 IP 地址。
#   -P|--port <int>|3306: 连接 drds 逻辑库的目标端口。
#   -u|--user <str>: 连接 drds 逻辑库的用户名。
#   -p|--password <str>: 连接 drds 逻辑库的用户密码。
#   -D|--database <str>: 指定使用的 schema。
#   -f|--fuck: 是否使用严格模式，即对 SIMPLE SEQUENCE 会尝试更加精准的起始点。
#   -s|--in-simple <int>: SIMPLE SEQUENCE 的 START WITH 自增建议值。
#   -g|--in-group <int>: GORUP SEQUENCE 的 START WITH 自增建议值。


import sys
import logging
import pymysql


def init_logger_stream(level):
    """初始化日志 Logger，将日志显示至运行终端。

    Args:
        level <str>: 日志级别。
    """
    logging.basicConfig(
        level = getattr(logging, level.upper()),
        format = "%(asctime)s [%(levelname)s] %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger()
    log_format = logging.Formatter("%(asctimes[%(levelname)s]: %(message)s")
    handler_stream = logging.StreamHandler()
    handler_stream.setLevel(getattr(logging, level.upper()))
    logger.addHandler(handler_stream)



class DRDSConnect(object):
    """
    """
    def __init__(self, host, user, password, port=3306, database=None):
        self.connect = None
        if not self.connect:
            self.connect = self.get_connect(
                host=host, 
                port=port, 
                user=user, 
                password=password, 
                database=database
            )
            
    def chk_connect(func):
        def ware(self, *args, **kwargs):
            if not self.connect:
                self.connect = self.get_connect(
                    host=host, 
                    port=port, 
                    user=user, 
                    password=password, 
                    database=database
                )
            else:
                self.connect.ping(reconnect=True)
            return func(self, *args, **kwargs)
        return ware
    
    def get_connect(self, host, user, password, port, database):
        """
        """
        # 检查输入参数
        is_ok = True
        if "" in (host, user):
            logging.error("Your input args {host, user} has empty format.")
            is_ok = False
        if not isinstance(port, int) or not 0 <= port < 65535:
            logging.error("Your input args {port} not is between 0 and 65535.")
            is_ok = False
        if not is_ok:
            sys.exit(1)

        connect = pymysql.connect(
            host = host,
            port = port,
            user = user,
            password = password,
            db = database,
            cursorclass = pymysql.cursors.DictCursor
        )
        return connect
        
    @chk_connect
    def show_sequences(self):
        """
        NAME | VALUE | INCREMENT_BY | START_WITH | MAX_VALUE | CYCLE | TYPE
        """
        sql = "show sequences;"
        with self.connect.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchall()
        self.connect.commit()
        return res
        
    @staticmethod
    def dump_sequences(seq_lst, is_fuck=False, in_simple=100, in_group=100):
        """
        create SIMPLE sequence SX_ALL start with 100016 increment by 1 maxvalue 999999 cycle;
        create SIMPLE sequence ZY_ALL start with 100008 increment by 1 maxvalue 999999 cycle;
        create SIMPLE sequence CFCA_ALL start with 10000124 increment by 1 maxvalue 99999999 cycle;
        create GROUP sequence SME_CREDIT_INFO_ALL start with 200004;
        create GROUP sequence LEGAL_CUS_INFO_ALL start with 200004;
        create GROUP sequence MSG_REC_ALL start with 500137;
        """
        template_sql = "create {type} sequence {name} {startwith} {increment} {maxvalue} {cycle}"
        res_lst = []
        for i in seq_lst:
            type = i["TYPE"]
            name = i["NAME"]
            value = int(i["VALUE"]) + (in_simple if type == "SIMPLE" else in_group)
            startwith = "start with {!s}".format(value)
            increment = "increment by {!s}".format(i["INCREMENT_BY"]) if i["INCREMENT_BY"] != "N/A" else ""
            maxvalue = "maxvalue {!s}".format(i["MAX_VALUE"]) if i["MAX_VALUE"] != "N/A" else ""
            cycle = "cycle" if i["CYCLE"] != "N/A" else ""
            dump_sql = template_sql.format(
                type = type,
                name = name,
                startwith = startwith,
                increment = increment,
                maxvalue = maxvalue,
                cycle = cycle
            ).strip() + ";"
            print(dump_sql)
            # res_lst.append(dump_sql)
        
            
            
    

    
    def __del__(self):
        """
        """
        if self.connect:
            self.connect.commit()
            self.connect.close()
        
        
        
def init_argparse():
    """
    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.description = "DRDS Sequences Dump Tool. -- Author: AcidGo"
    parser.add_argument(
        "-H", 
        "--host", 
        help="-H|--host <str>: The address for connect target db.", 
        type=str,
        required=True
    )
    parser.add_argument(
        "-P", 
        "--port", 
        help="-P|--port <3306/int>: The port for connect target db.", 
        type=int,
        default=3306
    )
    parser.add_argument(
        "-u", 
        "--user", 
        help="-u|--user <str>: The user for connect target db.", 
        type=str,
        required=True
    )
    parser.add_argument(
        "-p", 
        "--password", 
        help="-p|--password <str>: The password for connect target db.", 
        type=str,
        required=True
        )
    parser.add_argument(
        "-D", 
        "--database", 
        help="-D|--database <str>: Chose one schema.", 
        type=str
    )
    parser.add_argument(
        "-f",
        "--fuck",
        help="-f|--fuck: Use the fuck mode.",
        action="store_true"
    )
    parser.add_argument(
        "-s",
        "--in-simple",
        help="-s|--in-simple <int>: Simple sequence increment number.",
        type=int
    )
    parser.add_argument(
        "-g",
        "--in-group",
        help="-s|--in-group <int>: Group sequence increment number.",
        type=int
    )
    return parser.parse_args()
    
    
    
if __name__ == "__main__":
    args = init_argparse()
    drds = DRDSConnect(host=args.host, user=args.user, password=args.password)
    res = drds.show_sequences()
    DRDSConnect.dump_sequences(res)