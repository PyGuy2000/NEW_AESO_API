import os

def print_directory_tree(root_dir, padding=''):
    # Print the root directory
    print(padding[:-1] + '+--' + os.path.basename(root_dir) + '/')

    # Get the list of files and directories in the root directory
    contents = os.listdir(root_dir)

    # Recursively print the directory tree structure for each subdirectory
    for item in contents:
        item_path = os.path.join(root_dir, item)
        if os.path.isdir(item_path):
            print_directory_tree(item_path, padding + '    ')
        else:
            print(padding + '|--' + item)



print_directory_tree(r'C:\Users\kaczanor\OneDrive - Enbridge Inc\Documents\Python\Revised-AESO-API-master')
print(f" Working Directory: {os.getcwd()}") 