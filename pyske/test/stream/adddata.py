# Add data to the testdata_stream.txt file to test the stream get value function
i = 0
while True:
    i += 1
    with open("testdata_stream.txt", "a") as file_object:
        file_object.write(str(i) + "\n")
    file_object.close()
