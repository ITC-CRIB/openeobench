import sys


def create_crontab(filename: str, offset: int=5, period: int=3) -> list[str]:
    with open(filename, 'r', encoding='utf-8') as file:
        scripts = [line.strip() for line in file.readlines()]

    rows = []

    for i, script in enumerate(scripts):
        if not script:
            continue
        offset_min = i * offset
        offset_hour = offset_min // 60
        offset_minute = offset_min % 60

        base_hours = [h + offset_hour for h in range(0, 24, 3)]
        base_hours = [h for h in base_hours if h < 24]

        hour_list = ",".join(str(h) for h in base_hours)

        rows.append(f"{offset_minute:02d} {hour_list} * * * {script} >> /home/crib/openeo-checker.log 2>&1")

    return rows

if __name__ == '__main__':
    num_args = len(sys.argv)

    if num_args < 2:
        print("No filename.")
        exit(1)
    else:
        filename = sys.argv[1]

    if num_args > 2:
        offset = int(sys.argv[2])
    else:
        offset = 5

    if num_args > 3:
        period = int(sys.argv[3])
    else:
        period = 3

    rows = create_crontab(filename, offset=offset, period=period)

    print("\n".join(rows))
