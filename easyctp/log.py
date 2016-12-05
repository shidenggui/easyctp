# coding:utf8
import logging

log = logging.getLogger('easyctp')
log.setLevel(logging.DEBUG)
log.propagate = False

fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s %(lineno)s: %(message)s')
ch = logging.StreamHandler()

ch.setFormatter(fmt)
log.handlers.append(ch)
