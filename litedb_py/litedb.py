import re


def parse_query():
    first_line = "SELECT SUM(B.c1), SUM(B.c4)"
    second_line = "FROM A, B, C, E"
    third_line = "WHERE A.c2 = C.c0 AND A.c1 = B.c0 AND C.c1 = E.c0"
    fourth_line = "AND C.c1 < -4;"

    # first
    pattern_rc = r"(?:(\w)\.([\w\d]+))+"
    first_result = re.findall(pattern_rc, first_line)
    # print(first_result)

    # second
    pattern_second = r"(?:(?:, )?(\w))"
    second_result = re.findall(pattern_second, second_line[4:])
    # print(second_result)

    # third line
    third_result = re.findall(pattern_rc, third_line)
    # print(third_result)


if __name__ == '__main__':
    parse_query()
