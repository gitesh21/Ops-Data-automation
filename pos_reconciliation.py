import pandas as pd

def load_data(pos_file, cashout_file):
    pos = pd.read_csv(pos_file)
    cashout = pd.read_csv(cashout_file)
    return pos, cashout

def reconcile(pos, cashout):
    merged = pos.merge(
        cashout,
        on=["date", "store_id"],
        how="left",
        suffixes=("_pos", "_cash")
    )

    merged["difference"] = merged["total_pos"] - merged["total_cash"]
    merged["flag"] = merged["difference"].abs() > 5  # tolerance threshold

    return merged

def generate_report(df):
    summary = df.groupby("store_id").agg(
        total_days=("date", "count"),
        flagged_days=("flag", "sum")
    ).reset_index()

    return summary

if __name__ == "__main__":
    pos_data, cashout_data = load_data(
        "data/pos.csv",
        "data/cashout.csv"
    )

    reconciled = reconcile(pos_data, cashout_data)
    report = generate_report(reconciled)

    reconciled.to_csv("output/reconciled_report.csv", index=False)
    report.to_csv("output/summary.csv", index=False)

    print("Reconciliation completed.")

