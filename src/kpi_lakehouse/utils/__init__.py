"""Shared runtime utilities every job in this project uses: read, validate, write, dates.

A job whose output has a Hive table also registers the partitions it wrote (catalog.py).
"""
