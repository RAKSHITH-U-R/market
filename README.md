# Approach

hotness_score = (sold_homes_count / new_listings_count) * (homes_sold_over_list_price_count / sold_homes_count) * (1 - (median_sale_to_list_ratio)) * (1 / days_to_sell)
The idea behind this calculation is that a market with a high "hotness score" would have:

1. A high number of sold homes compared to new listings, indicating high demand
2. A high percentage of homes sold above the list price, indicating strong competition and bidding wars
3. A low median sale to list ratio, indicating that homes are selling for close to or above the list price
4. A short time to sell, indicating that homes are quickly being snapped up by buyers