SELECT
    id
  , name
  , description
  , release_date
  , duration_ms
  , languages
  , explicit
  , type
  , CURRENT_DATETIME('America/Sao_Paulo') AS datahora_carga
FROM
  `singular-arcana-383119.refined_api.spotify_podcast_episodes`
WHERE
  UPPER(description) LIKE ("%BOTICARIO%")