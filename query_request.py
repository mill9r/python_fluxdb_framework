class InfluxDBQuery:
    def append_clause(self, name, clause=None):

        additional_param = []

        if clause != None and len(clause) != 0:
            additional_param.append(name)
            additional_param.append(clause)

        return additional_param

    # TODO string -> list
    # def cast_string_to_list(self,str):
    #     if len(str) == 1:
    #         return [str]
    #     else:
    #         return str


    # TODO improve code
    def set_time_for_report_from_to(self,
                                    where_clause=None,
                                    time_from=None,
                                    time_to=None
                                    ):

        span_of_time = []

        if time_from != None and len(time_from) != 0:
            span_of_time.append(
                ''.join(self.append_clause("time >= ", time_from))
            )

        if time_to != None and len(time_to) != 0:
            span_of_time.append(
                ''.join(self.append_clause(" AND time <= ", time_to))
            )
        #TODO check if string not empty
        if where_clause != None and len(where_clause) != 0:
            for item in where_clause:
                span_of_time.append(
                    ''.join(self.append_clause(" AND ", item))
                )

        print  span_of_time
        return ''.join(span_of_time)

    def query_select(self,
                     function,
                     measurement,
                     tag_value='value',
                     where_clause=None,
                     time_from=None,
                     time_to=None,
                     group_by=None):

        if measurement is None:
            measurement = []

        if tag_value is None:
            tag_value = []

        if group_by is None:
            group_by = []

        query = []

        # function = self.cast_string_to_list(function)
        # print function

        for function_item in function:
            query.append("SELECT {0}({1}) FROM {2}{3}{4};".format(
                function_item,
                tag_value,
                measurement,
                ' '.join(self.append_clause(" WHERE",
                                            self.set_time_for_report_from_to(time_from=time_from, time_to=time_to,
                                                                             where_clause=where_clause))),
                ' '.join(self.append_clause(" GROUP BY", group_by))
            ))

        return query
