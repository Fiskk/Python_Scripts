# Your  data must be numeric and in ASC order and have a header for this to work.
# Change the string on line 6 to be your column header name, which you must have.
import pandas as pd

df = pd.read_csv(r'C:\Users\ssampson\Documents\Python_Scripts\ON_List.csv', delimiter=',')
full_list = df['EBELN'].tolist()

# print(df.head(3))

Array_of_arrays = []
cumulative_diff_limit = 1000

temp_array = []
cumulative_diff = 0
for x in range(0, len(full_list)-1):

    difference = abs(full_list[x] - full_list[x+1])
    cumulative_diff += difference

    if cumulative_diff <= cumulative_diff_limit:
        temp_array.append(full_list[x])
    else:
        cumulative_diff = 0
        temp_array.append(full_list[x])
        Array_of_arrays.append(temp_array)

        temp_array = []

    if x == len(full_list)-2:
        temp_array.append(full_list[x+1])
        Array_of_arrays.append(temp_array)

# Min in group - Max in group : count : max - min

num_of_values = 0
for x in Array_of_arrays:
    print(min(x), ' - ', max(x), ' : ', len(x), ' : ', max(x) - min(x))
    num_of_values += len(x)

print('Number of pairs ', len(Array_of_arrays))
print('Number of values', num_of_values)

# This will print out each array

# for x in Array_of_arrays:
#     print(x)
