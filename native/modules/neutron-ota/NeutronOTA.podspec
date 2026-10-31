require 'json'
package = JSON.parse(File.read(File.join(__dir__, 'package.json')))
Pod::Spec.new do |s|
  s.name = 'NeutronOTA'
  s.version = package['version']
  s.summary = 'Authenticated durable Neutron OTA boot selection'
  s.homepage = 'https://github.com/neutron-build/neutron'
  s.license = 'MIT'
  s.author = 'Neutron'
  s.source = { :git => 'https://github.com/neutron-build/neutron.git', :tag => s.version.to_s }
  s.platforms = { :ios => '15.1' }
  s.source_files = 'ios/**/*.{h,m,mm,swift}'
  s.swift_version = '5.9'
  install_modules_dependencies(s)
end
