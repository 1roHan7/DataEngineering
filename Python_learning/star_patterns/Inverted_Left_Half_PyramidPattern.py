# *****
#  ****
#   ***
#    **
#     *

def inverted_left_half_pyramid(n):
    
    for i in range (1,n+1):
        for j in range(1,i):
            print(" " ,end="")
        for j in range(1,n+2-i):
            print('*',end="")
        print()
inverted_left_half_pyramid(5)

