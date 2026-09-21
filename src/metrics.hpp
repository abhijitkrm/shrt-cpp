#pragma once
#include <string>

namespace shrt::metrics {

void init();
void tick();
/// Renders {"req_s","total","uptime_s","per_second":[31]}.
std::string snapshot();

} // namespace shrt::metrics
