# -*- mode: ruby -*-
# vi: set ft=ruby :


# This sets up a VM on which to hack on Trck.
#
# Interaction with the source should all be done on the host
# machine. This includes managing the git submodules.
#
# Usage:
#
# 1. host $ vagrant up
# 2. host $ vagrant ssh
# 3. vm   $ cd /vagrant_data
# 4. vm   $ make test


Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/xenial64"

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # config.vm.network "forwarded_port", guest: 80, host: 8080

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  config.vm.network "private_network", ip: "192.168.33.10"

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  config.vm.synced_folder ".", "/vagrant_data"

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  #
  config.vm.provider "virtualbox" do |vb|
    # Display the VirtualBox GUI when booting the machine
    vb.gui = false
  
    # Customize the amount of memory on the VM:
    vb.memory = "1024"
  end

  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.
  config.vm.provision "shell", inline: <<-SHELL
    set -e

    apt update
    apt install -y \
      autoconf \
      bash \
      build-essential \
      clang \
      cmake \
      gcc \
      jq \
      libarchive-dev \
      libc6-dev \
      libcmph-dev \
      libcurl4-openssl-dev \
      libjson-c-dev \
      libjudy-dev \
      libtcmalloc-minimal4  \
      libtool \
      make \
      ntp \
      pkg-config \
      python \
      python-ply

    echo 'export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH' >/etc/profile.d/ld_library_path.sh
    chmod +x /etc/profile.d/ld_library_path.sh
    source /etc/profile.d/ld_library_path.sh

    cd /vagrant_data/deps/traildb
    ./waf clean
    ./waf configure
    ./waf build
    sudo ./waf install

    cd /vagrant_data/deps/msgpack-c
    cmake .
    make clean
    make
    sudo make install
  SHELL
end
