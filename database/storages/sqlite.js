/* eslint-disable camelcase */
const { VS_DB_URL, VS_DB_PATH } = process.env

/**
 * @type { import('better-sqlite3').Database }
 */
let db

function connectDatabase() {
  if (db) return db

  // Determine database path from VS_DB_PATH or VS_DB_URL
  let dbPath = VS_DB_PATH || './visit_stat.db'
  if (VS_DB_URL && VS_DB_URL.startsWith('sqlite://')) {
    dbPath = VS_DB_URL.replace('sqlite://', '')
  }

  const Database = require('better-sqlite3')
  db = new Database(dbPath)

  // Create table if not exists
  db.exec(`
    CREATE TABLE IF NOT EXISTS visit_stat (
      id TEXT PRIMARY KEY,
      site_pv INTEGER DEFAULT 0,
      site_uv TEXT DEFAULT '[]',
      page_pv INTEGER DEFAULT 0,
      page_uv TEXT DEFAULT '[]'
    )
  `)

  return db
}

module.exports = async () => {
  connectDatabase()

  return {
    /**
     * get visits
     * @param { [string, any][] } urlEntries
     * @returns { {url?: string; site_pv?: number; site_uv?: number; page_pv?: number; page_uv?: number; } }
     */
    async visits(urlEntries) {
      const fromEntries = Object.fromEntries(urlEntries)
      const ids = urlEntries.map(([url]) => url)

      const placeholders = ids.map(() => '?').join(',')
      const query = `SELECT * FROM visit_stat WHERE id IN (${placeholders})`
      const result = db.prepare(query).all(...ids)

      const visits = result.map((item) => {
        const obj = { url: fromEntries[item.id] }
        if (item.site_pv) obj.site_pv = item.site_pv
        if (item.site_uv) {
          const uvArray = JSON.parse(item.site_uv)
          obj.site_uv = uvArray.length
        }

        if (item.page_pv) obj.page_pv = item.page_pv
        if (item.page_uv) {
          const uvArray = JSON.parse(item.page_uv)
          obj.page_uv = uvArray.length
        }
        return obj
      })
      return visits
    },

    /**
     * get counter
     * @param { string } ip
     * @param { string } host
     * @param { string } referer
     * @returns { {site_pv: number; site_uv: number; page_pv?: number; page_uv?: number; } }
     */
    async counter(ip, host, referer) {
      // Use transaction for atomic operations
      const updateCounter = db.transaction((ip, host, referer) => {
        // Helper function to add unique IP
        const addUniqueIP = (id, ip, field) => {
          const row = db.prepare('SELECT * FROM visit_stat WHERE id = ?').get(id)

          if (!row) {
            // Insert new record
            const uvArray = [ip]
            const pvCount = 1
            if (field === 'site_uv') {
              db.prepare('INSERT INTO visit_stat (id, site_pv, site_uv) VALUES (?, ?, ?)').run(
                id,
                pvCount,
                JSON.stringify(uvArray)
              )
            } else {
              db.prepare('INSERT INTO visit_stat (id, page_pv, page_uv) VALUES (?, ?, ?)').run(
                id,
                pvCount,
                JSON.stringify(uvArray)
              )
            }
          } else {
            // Update existing record
            const uvArray = JSON.parse(row[field] || '[]')
            const pvField = field === 'site_uv' ? 'site_pv' : 'page_pv'
            const pvCount = (row[pvField] || 0) + 1

            if (!uvArray.includes(ip)) {
              uvArray.push(ip)
            }

            db.prepare(`UPDATE visit_stat SET ${pvField} = ?, ${field} = ? WHERE id = ?`).run(
              pvCount,
              JSON.stringify(uvArray),
              id
            )
          }
        }

        // Update host statistics
        addUniqueIP(host, ip, 'site_uv')

        // Update referer statistics if different from host
        if (host !== referer) {
          addUniqueIP(referer, ip, 'page_uv')
        }

        // Fetch results
        const host_result = db.prepare('SELECT * FROM visit_stat WHERE id = ?').get(host)
        const obj = {
          site_pv: host_result.site_pv,
          site_uv: JSON.parse(host_result.site_uv).length
        }

        if (host !== referer) {
          const referer_result = db.prepare('SELECT * FROM visit_stat WHERE id = ?').get(referer)
          if (referer_result) {
            obj.page_pv = referer_result.page_pv
            obj.page_uv = JSON.parse(referer_result.page_uv).length
          }
        }

        return obj
      })

      return updateCounter(ip, host, referer)
    }
  }
}
