CREATE or replace TABLE `data-uc-fra-dev.cpa.d_slicers_values` AS

SELECT 
"1" as `ORDER`,
"email" as Type,
"monthly_recap_tx" as Slicer,
"Taux d'ouverture" as `Filter`
UNION ALL

SELECT
"2" as `ORDER`,
"email" as Type,
"monthly_recap_tx" as Slicer,
"Taux de clic" as `Filter`
UNION ALL

SELECT
"3" as `ORDER`,
"email" as Type,
"monthly_recap_tx" as Slicer,
"Taux de réactivité" as `Filter`
UNION ALL

SELECT
"4" as `ORDER`,
"email" as Type,
"monthly_recap_tx" as Slicer,
"Taux de désabonnement" as `Filter`
UNION ALL

SELECT
"1" as `ORDER`,
"email" as Type,
"monthly_recap_topflop" as Slicer,
"Réceptions" as `Filter`

UNION ALL
SELECT
"2" as `ORDER`,
"email" as Type,
"monthly_recap_topflop" as Slicer,
"Ouvertures" as `Filter`

UNION ALL
SELECT
"3" as `ORDER`,
"email" as Type,
"monthly_recap_topflop" as Slicer,
"Clics" as `Filter`

UNION ALL
SELECT
"4" as `ORDER`,
"email" as Type,
"monthly_recap_topflop" as Slicer,
"Réactivité" as `Filter`

UNION ALL
SELECT
"5" as `ORDER`,
"email" as Type,
"monthly_recap_topflop" as Slicer,
"Désabonnements" as `Filter`

UNION ALL
SELECT
"1" as `ORDER`,
"email" as Type,
"monthly_dash_interactions" as Slicer,
"Ouvertures" as `Filter`

UNION ALL
SELECT
"2" as `ORDER`,
"email" as Type,
"monthly_dash_interactions" as Slicer,
"Clics" as `Filter`

UNION ALL
SELECT
"3" as `ORDER`,
"email" as Type,
"monthly_dash_interactions" as Slicer,
"Réactivité" as `Filter`

UNION ALL
SELECT
"4" as `ORDER`,
"email" as Type,
"monthly_dash_interactions" as Slicer,
"Désabonnements" as `Filter`

UNION ALL
SELECT
"5" as `ORDER`,
"email" as Type,
"monthly_dash_interactions" as Slicer,
"Plaintes" as `Filter`

UNION ALL
SELECT
"1" as `ORDER`,
"email" as Type,
"multi_routing_bounce" as Slicer,
"Bounces" as `Filter`

UNION ALL
SELECT
"2" as `ORDER`,
"email" as Type,
"multi_routing_bounce" as Slicer,
"SoftBounces" as `Filter`

UNION ALL
SELECT
"3" as `ORDER`,
"email" as Type,
"multi_routing_bounce" as Slicer,
"HardBounces" as `Filter`

UNION ALL
SELECT
"4" as `ORDER`,
"email" as Type,
"multi_routing_bounce" as Slicer,
"BlockBounces" as `Filter`

UNION ALL
SELECT
"5" as `ORDER`,
"email" as Type,
"multi_routing_bounce" as Slicer,
"TechnicalBounces" as `Filter`

UNION ALL
SELECT
"6" as `ORDER`,
"email" as Type,
"multi_routing_bounce" as Slicer,
"UnknownBounces" as `Filter`

UNION ALL
SELECT
"1" as `ORDER`,
"email" as Type,
"evolution_campaign_interactions" as Slicer,
"Réceptions" as `Filter`

UNION ALL
SELECT
"2" as `ORDER`,
"email" as Type,
"evolution_campaign_interactions" as Slicer,
"Réactivité" as `Filter`

UNION ALL
SELECT
"3" as `ORDER`,
"email" as Type,
"evolution_campaign_interactions" as Slicer,
"Ouvertures" as `Filter`

UNION ALL
SELECT
"4" as `ORDER`,
"email" as Type,
"evolution_campaign_interactions" as Slicer,
"Clics" as `Filter`

UNION ALL
SELECT
"5" as `ORDER`,
"email" as Type,
"evolution_campaign_interactions" as Slicer,
"Plaintes" as `Filter`

UNION ALL
SELECT
"6" as `ORDER`,
"email" as Type,
"evolution_campaign_interactions" as Slicer,
"Désabonnements" as `Filter`
