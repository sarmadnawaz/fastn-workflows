export default async function (ctx) {
  // Self-contained major timezone list (name + fixed UTC offset), ported from the
  // v1 Helpers getTimeZones connector which returned { timezones: [{ name, utc_offset }] }.
  const timezones = [
    { name: "Pacific/Midway", utc_offset: "-11:00" },
    { name: "Pacific/Honolulu", utc_offset: "-10:00" },
    { name: "America/Anchorage", utc_offset: "-09:00" },
    { name: "America/Los_Angeles", utc_offset: "-08:00" },
    { name: "America/Denver", utc_offset: "-07:00" },
    { name: "America/Chicago", utc_offset: "-06:00" },
    { name: "America/New_York", utc_offset: "-05:00" },
    { name: "America/Halifax", utc_offset: "-04:00" },
    { name: "America/Sao_Paulo", utc_offset: "-03:00" },
    { name: "Atlantic/South_Georgia", utc_offset: "-02:00" },
    { name: "Atlantic/Azores", utc_offset: "-01:00" },
    { name: "UTC", utc_offset: "+00:00" },
    { name: "Europe/London", utc_offset: "+00:00" },
    { name: "Europe/Paris", utc_offset: "+01:00" },
    { name: "Europe/Athens", utc_offset: "+02:00" },
    { name: "Europe/Moscow", utc_offset: "+03:00" },
    { name: "Asia/Dubai", utc_offset: "+04:00" },
    { name: "Asia/Karachi", utc_offset: "+05:00" },
    { name: "Asia/Kolkata", utc_offset: "+05:30" },
    { name: "Asia/Dhaka", utc_offset: "+06:00" },
    { name: "Asia/Bangkok", utc_offset: "+07:00" },
    { name: "Asia/Shanghai", utc_offset: "+08:00" },
    { name: "Asia/Tokyo", utc_offset: "+09:00" },
    { name: "Australia/Sydney", utc_offset: "+10:00" },
    { name: "Pacific/Noumea", utc_offset: "+11:00" },
    { name: "Pacific/Auckland", utc_offset: "+12:00" }
  ];
  const options = timezones.map((tz) => ({
    label: `${tz.name}- ${tz.utc_offset}`,
    value: tz.utc_offset
  }));
  return { options };
}
