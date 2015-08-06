#!/usr/bin/ruby

once = false
File.foreach(ARGV.shift) do |line|
  if line =~ /BEFORE/
    puts "  " + line
  elsif line =~ /AFTER/
    puts "  " + line
    if (once)
      once = false
      puts
    else
      once = true
    end
  elsif line =~ /schedule_new_message/
    puts line
    puts
  end
end
