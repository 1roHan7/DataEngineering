#     *
#    ***
#   *****
#  *******
# *********

def full_pyramid_pattern(n):
    l=0
    for i in range (1,n+1):
        
        for j in range(1, n - i + 1):
            print(" ", end="")
        for k in range(1,i+l+1):
            print("*",end="")
            l=i
        print()

full_pyramid_pattern(5)