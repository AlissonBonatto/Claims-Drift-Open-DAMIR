import os 

parent_path = os.path.dirname("/raid/datasets/allianzsante/")
for page in range(1, 7):
    path = os.path.join(parent_path, f"page{page}")
    if not os.path.exists(path):
        os.makedirs(path)
