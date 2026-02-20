import polars as pl

def contains_any(column,values):
    return pl.col(column).list.eval(
        pl.element().is_in(values)
    ).list.any()
    
def contains_all(column,values):
    return pl.col(column).list.eval(
            pl.element().is_in(values)
        ).list.sum() == len(values)
    
def single_among(column,values):
    return (
        (pl.col(column).list.len() == 1) &
        (pl.col(column).list.first().is_in(values)))

def contains_exact(column,values):
    return pl.col(column).list.sort() == sorted(values)