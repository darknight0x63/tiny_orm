def _conditions_to_sql(conditions: list) -> str:
    cond_str = str()
    _values = []
    for cond in conditions:
        if type(cond) == str and cond in ["&", "|"]:
            cond_str += " %s" % "AND" if cond == '&' else "OR"
        elif len(cond) == 3:
            cond_str += " %s%s%s" % (cond[0], cond[1], "%s")
            _values.append(cond[2])
        else:
            raise ValueError("Invalid expression %s" % cond)

    if cond_str:
        cond_str = "WHERE " + cond_str

    return cond_str, _values
