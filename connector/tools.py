def _conditions_to_sql(conditions: list) -> str:
    cond_str = str()
    _values = []
    for cond in conditions:
        if type(cond) == str and cond in ["&", "|"]:
            cond_str += " %s" % "AND" if cond == '&' else "OR"
        elif len(cond) == 3:
            operand_1, operator, operand_2 = cond[0], cond[1], cond[2]
            if operator.lower() == "in" or operator.lower() == "not in":
                cond_str += " %s %s %s" % \
                            (operand_1,
                             operator,
                             "( " + ", ".join([("'%s'" % it) if it is not None else "NULL" for it in operand_2]) + " )")
            else:
                cond_str += " %s %s %s" % (operand_1, operator, "%s")
                _values.append(operand_2)
        else:
            raise ValueError("Invalid expression %s" % cond)

    if cond_str:
        cond_str = "WHERE" + cond_str

    return cond_str, _values
