package com.bjke.flink.model

case class RecordDetail(id: Int, created_at: String, updated_at: String, deleted_at: String, date: Long, player_id: Int, player_name: String, seat: Int, roleId: Int,
                        choose: Int, power_wolf: Int, operation_id: Int,
                        day: Int, season_id: Int, group: Int, score: Double, points: Double, egg: Double, active_score: Double, total: Double, suicide: Boolean,
                        bottom: Boolean, topVote: Int, record_id: Int, season_type_id: Int, deaths_id: Int, round: Int) {
}
