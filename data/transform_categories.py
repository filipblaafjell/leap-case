import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import numpy as np


df = pd.read_csv("data/amazon/amazon-purchases.csv")


unique_codes = [c for c in df["Category"].unique().tolist() if pd.notna(c)]
print(len(unique_codes), "unique non-null category codes found.")


model = SentenceTransformer("all-MiniLM-L6-v2")

codes_text = [str(c).replace("_", " ").title() for c in unique_codes]


embeddings = model.encode(codes_text, normalize_embeddings=True)


amazon_top = [
    "Electronics", "Home & Kitchen", "Clothing, Shoes & Jewelry",
    "Beauty & Personal Care", "Sports & Outdoors", "Automotive",
    "Toys & Games", "Grocery", "Health & Household",
    "Office Products", "Pet Supplies", "Tools & Home Improvement",
    "Books", "Music", "Movies & TV"
]
amazon_emb = model.encode(amazon_top, normalize_embeddings=True)


sims = cosine_similarity(embeddings, amazon_emb)


THRESHOLD = 0.3
mapping = {}
for i, code in enumerate(unique_codes):
    best_idx = np.argmax(sims[i])
    best_score = sims[i][best_idx]
    if best_score >= THRESHOLD:
        mapping[code] = amazon_top[best_idx]
    else:
        mapping[code] = "Unknown"

print("\nTop 10 sample mappings (with confidence):")
for i in range(min(10, len(unique_codes))):
    top_idx = np.argsort(sims[i])[::-1][:3]
    best = top_idx[0]
    print(f"{unique_codes[i]} -> {amazon_top[best]} ({sims[i][best]:.2f})")
    print("  top 3:", [f"{amazon_top[j]} ({sims[i][j]:.2f})" for j in top_idx], "\n")

df["Mapped_Category"] = df["Category"].map(mapping)
df.to_csv("data/amazon/amazon-purchases_mapped.csv", index=False)

unknown_count = sum(v == "Unknown" for v in mapping.values())
print(f"Mapping complete. File saved as 'amazon-purchases_mapped.csv'.")
print(f"{unknown_count} / {len(mapping)} categories ({unknown_count/len(mapping)*100:.2f}%) labeled as Unknown (below threshold {THRESHOLD}).")
