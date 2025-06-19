from simulation.data_loaders import load_sp500, load_stocks, load_betas

def initialize_cache():
    print("\n=== Loading SP500 table ===")
    sp500_df = load_sp500(force_reload=True)
    print(f"SP500 rows loaded: {len(sp500_df)}")

    print("\n=== Loading Stocks table ===")
    stocks_df = load_stocks(force_reload=True)
    print(f"Stocks rows loaded: {len(stocks_df)}")

    print("\n=== Loading Betas table ===")
    betas_df = load_betas(force_reload=True)
    print(f"Betas rows loaded: {len(betas_df)}")

    print("\n✅ Cache initialization complete.")

if __name__ == "__main__":
    initialize_cache()
