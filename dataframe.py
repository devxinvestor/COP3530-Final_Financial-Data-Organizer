from hashmap import MyHashMap
import datetime

class MyDataFrame:
    def __init__(self, sorted_columns=None, sorted_values=None):
        self.data = MyHashMap()
        self.columns = []

        if sorted_columns and sorted_values:
            for column, value in zip(sorted_columns, sorted_values):
                self.__setitem__(column, value)

    def __repr__(self):
        return f"MyDataFrame(data={self.data}, columns={self.columns})"

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

    def drop(self, column_name):
        if column_name not in self.data:
            raise KeyError(f"Column '{column_name}' does not exist.")
        self.data.remove(column_name)
        self.columns.remove(column_name)
    
    def rename(self, columns):
        for old_name, new_name in columns.items():
            if old_name not in self.data:
                raise KeyError(f"Column '{old_name}' does not exist.")
            if new_name in self.data:
                raise ValueError(f"Column '{new_name}' already exists.")
            temp_value = self.data[old_name]
            self.data[new_name] = temp_value
            self.data.remove(old_name)
            self.columns[self.columns.index(old_name)] = new_name