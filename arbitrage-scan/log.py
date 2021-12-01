# coding:utf-8
"""
@author: weitao.chu@certik.org
@time: 2021/11/08
"""
import logging

import colorlog


def set_log_format(name="", level=logging.INFO):
    log_date_format = "%Y-%m-%dT%H:%M:%S"
    handler = colorlog.StreamHandler()
    file_name_length = 33 - len(name)
    handler.setFormatter(
        colorlog.ColoredFormatter(
            f"%(asctime)s.%(msecs)03d {name} %(name)-{file_name_length}s: "
            f"%(log_color)s%(levelname)-8s%(reset)s %(message)s",
            datefmt=log_date_format,
            reset=True,
        )
    )

    logger = colorlog.getLogger()
    logger.addHandler(handler)
    logger.setLevel(level)

