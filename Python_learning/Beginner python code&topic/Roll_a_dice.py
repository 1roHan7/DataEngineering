from random import randint

play = input("are you ready to play? y/n")

if play == 'y' or play=='Y' :
    print (f" {randint(1,6)} ,{randint(1,6)} are the mumbers")



elif play == 'n' or play == 'N':
    print ("Thanks for playing")


else:
    print("Invalid Input")

