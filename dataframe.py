from dictionary import MyDictionary

class MyDataFrame:
    def __init__(self):
        self.data = MyDictionary()
        self.columns = []

    def __setitem__(self, column_name, values):
        if column_name in self.data:
            raise ValueError(f"Column '{column_name}' already exists.")
        self.data[column_name] = values
        self.columns.append(column_name)

    def __getitem__(self, column_name):
        if column_name not in self.data:
            raise KeyError(f"Column '{column_name}' does not exist.")
        return self.data[column_name]

    def add_row(self, row):
        if len(row) != len(self.columns):
            raise ValueError("Row length must match the number of columns.")
        for column, value in zip(self.columns, row):
            self.data[column].append(value)

    def get_row(self, index):
        if index < 0 or index >= self.row_count():
            raise IndexError("Row index out of range.")
        return [self.data[column][index] for column in self.columns]

    def row_count(self):
        if self.columns:
            return len(self.data[self.columns[0]])
        return 0

    def __str__(self):
        return self._format_as_table()

    def __repr__(self):
        return self._format_as_table()

    def _format_as_table(self):
        if not self.columns:
            return "DataFrame is empty."
        
        header = " | ".join(self.columns)
        separator = "-" * len(header)
        rows = []

        for i in range(self.row_count()):
            row = self.get_row(i)
            rows.append(" | ".join(map(str, row)))

        return f"{header}\n{separator}\n" + "\n".join(rows)
