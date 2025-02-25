"""
Script with hekper functions and objects for geosptail tiles,
such as OS Grid or Uner H3
"""
import math

# NOTE: this has been copied from the `lidar-processor` module.
def get_os_grid_reference(e, n, precision=10):
    """
    Convert OS easting/northing to OS Grid Reference.
    
    Args:
        e (float): Easting
        n (float): Northing
        precision (int): Precision level (6 for 100m, 8 for 10m, 10 for 1m)
        
    Returns:
        str: OS Grid Reference (e.g., "NT 25940 73060" for 1m precision)
    """
    # Note no I
    grid_chars = "ABCDEFGHJKLMNOPQRSTUVWXYZ"
    
    # get the 100km-grid indices
    e100k = math.floor(e/100000)
    n100k = math.floor(n/100000)
    
    if e100k < 0 or e100k > 6 or n100k < 0 or n100k > 12:
        return ''
    
    # translate those into numeric equivalents of the grid letters
    l1 = (19-n100k)-(19-n100k)%5+math.floor((e100k+10)/5)
    l2 = (19-n100k)*5%25 + e100k%5
    
    let_pair = grid_chars[int(l1)] + grid_chars[int(l2)]
    
    # Calculate the remaining digits based on precision
    # For easting
    e_remaining = e - (e100k * 100000)
    # For northing
    n_adjusted = n
    if n >= 1000000:
        n_adjusted = n - 1000000
    n_remaining = n_adjusted - (n100k * 100000)
    
    # Format based on precision
    if precision == 6:  # 100m precision
        # Floor to get correct grid square (avoid rounding up)
        e_digits = str(math.floor(e_remaining/100)).rjust(3, "0")
        n_digits = str(math.floor(n_remaining/100)).rjust(3, "0")
        return f"{let_pair}{e_digits}{n_digits}"
    elif precision == 8:  # 10m precision
        e_digits = str(math.floor(e_remaining/10)).rjust(4, "0")
        n_digits = str(math.floor(n_remaining/10)).rjust(4, "0")
        return f"{let_pair} {e_digits} {n_digits}"
    elif precision == 10:  # 1m precision
        e_digits = str(math.floor(e_remaining)).rjust(5, "0")
        n_digits = str(math.floor(n_remaining)).rjust(5, "0")
        return f"{let_pair} {e_digits} {n_digits}"
    else:
        raise ValueError("Precision must be 6, 8, or 10")