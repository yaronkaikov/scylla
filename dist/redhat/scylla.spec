Name:           %{product}
Version:        %{version}
Release:        %{release}
Summary:        Scylla is a highly scalable, eventually consistent, distributed, partitioned row DB.
Group:          Applications/Databases

License:        AGPLv3
URL:            http://www.scylladb.com/
Source0:        %{reloc_pkg}
Requires:       %{product}-server = %{version}-%{release}
Requires:       %{product}-conf = %{version}-%{release}
Requires:       %{product}-python3 = %{version}-%{release}
Requires:       %{product}-kernel-conf = %{version}-%{release}
Requires:       %{product}-jmx = %{version}-%{release}
Requires:       %{product}-tools = %{version}-%{release}
Requires:       %{product}-tools-core = %{version}-%{release}
Requires:       %{product}-node-exporter = %{version}-%{release}
Requires:       %{product}-cqlsh = %{version}-%{release}

Suggests:       %{product}-machine-image = %{version}-%{release}
Obsoletes:	scylla-server < 1.1

%global _debugsource_template %{nil}
%global _debuginfo_subpackages %{nil}
%global __brp_python_bytecompile %{nil}
%global __brp_mangle_shebangs %{nil}

%undefine _find_debuginfo_dwz_opts

# Prevent find-debuginfo.sh from tempering with scylla's build-id (#5881)
%undefine _unique_build_ids
%global _no_recompute_build_ids 1

# rpm causes missing build-id error on node_exporter, so ignore it
%undefine _missing_build_ids_terminate_build

%description
Scylla is a highly scalable, eventually consistent, distributed,
partitioned row DB.
This package installs all required packages for ScyllaDB,  including
%{product}-server, %{product}-jmx, %{product}-tools, %{product}-tools-core %{product}-node-exporter.

# this is needed to prevent python compilation error on CentOS (#2235)
%if 0%{?rhel}
%global __os_install_post    \
    /usr/lib/rpm/redhat/brp-compress \
    %{!?__debug_package:\
    /usr/lib/rpm/redhat/brp-strip %{__strip} \
    /usr/lib/rpm/redhat/brp-strip-comment-note %{__strip} %{__objdump} \
    } \
    /usr/lib/rpm/redhat/brp-strip-static-archive %{__strip} \
    %{!?__jar_repack:/usr/lib/rpm/redhat/brp-java-repack-jars} \
%{nil}
%endif

%files
%defattr(-,root,root)

%prep
%setup -q -n scylla

%package        server
Group:          Applications/Databases
Summary:        The Scylla database server
License:        AGPLv3
URL:            http://www.scylladb.com/
Requires:       %{product}-conf = %{version}-%{release}
Requires:       %{product}-python3 = %{version}-%{release}
Conflicts:      abrt
AutoReqProv:    no

%description server
This package contains ScyllaDB server.

%build

defines=()

%install
%if 0%{housekeeping}
install_arg="--housekeeping"
%endif
./install.sh --packaging --root "$RPM_BUILD_ROOT" $install_arg

%pre server
getent group scylla || /usr/sbin/groupadd scylla 2> /dev/null || :
getent passwd scylla || /usr/sbin/useradd -g scylla -s /sbin/nologin -r -d %{_sharedstatedir}/scylla scylla 2> /dev/null || :

%post server
/opt/scylladb/scripts/scylla_post_install.sh

if [ $1 -eq 1 ] ; then
    /usr/bin/systemctl preset scylla-server.service ||:
fi

%preun server
if [ $1 -eq 0 ] ; then
    /usr/bin/systemctl --no-reload disable scylla-server.service ||:
    /usr/bin/systemctl stop scylla-server.service ||:
fi

%postun server
/usr/bin/systemctl daemon-reload ||:

%posttrans server
if  [ -d /tmp/%{name}-%{version}-%{release} ]; then
    cp -a /tmp/%{name}-%{version}-%{release}/* /etc/scylla/
    rm -rf /tmp/%{name}-%{version}-%{release}/
fi
ln -sfT /etc/scylla /var/lib/scylla/conf

%clean
rm -rf $RPM_BUILD_ROOT

%files server
%defattr(-,root,root)

%config(noreplace) %{_sysconfdir}/sysconfig/scylla-server
%config(noreplace) %{_sysconfdir}/sysconfig/scylla-housekeeping
%attr(0755,root,root) %dir %{_sysconfdir}/scylla.d
%config(noreplace) %{_sysconfdir}/scylla.d/*.conf
/opt/scylladb/share/doc/scylla/*
%{_unitdir}/scylla-fstrim.service
%{_unitdir}/scylla-housekeeping-daily.service
%{_unitdir}/scylla-housekeeping-restart.service
%{_unitdir}/scylla-server.service
%{_unitdir}/*.timer
%{_unitdir}/*.slice
%{_bindir}/scylla
%{_bindir}/iotune
%{_bindir}/scyllatop
%{_sbindir}/scylla*
%{_sbindir}/node_health_check
%{_sbindir}/seastar-cpu-map.sh
/opt/scylladb/scripts/*
/opt/scylladb/swagger-ui/dist/*
/opt/scylladb/api/api-doc/*
/opt/scylladb/scyllatop/*
/opt/scylladb/bin/*
/opt/scylladb/libreloc/*
/opt/scylladb/libexec/*
%{_prefix}/lib/scylla/*
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla/
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla/data
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla/commitlog
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla/hints
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla/view_hints
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla/coredump
%attr(0755,scylla,scylla) %dir %{_sharedstatedir}/scylla-housekeeping
%ghost /etc/systemd/system/scylla-helper.slice.d/
%ghost /etc/systemd/system/scylla-helper.slice.d/memory.conf
%ghost /etc/systemd/system/scylla-server.service.d/capabilities.conf
%ghost /etc/systemd/system/scylla-server.service.d/mounts.conf
/etc/systemd/system/scylla-server.service.d/dependencies.conf
%ghost %config /etc/systemd/system/var-lib-systemd-coredump.mount
%ghost /etc/systemd/system/scylla-cpupower.service
%ghost %config /etc/systemd/system/var-lib-scylla.mount

%package conf
Group:          Applications/Databases
Summary:        Scylla configuration package
License:        AGPLv3
URL:            http://www.scylladb.com/
Obsoletes:	scylla-server < 1.1

%description conf
This package contains the main scylla configuration file.

# we need to refuse upgrade if current scylla < 1.7.3 && commitlog remains
%pretrans conf
ver=$(rpm -qi scylla-server | grep Version | awk '{print $3}')
if [ -n "$ver" ]; then
    ver_fmt=$(echo $ver | awk -F. '{printf "%d%02d%02d", $1,$2,$3}')
    if [ $ver_fmt -lt 10703 ]; then
        # for <scylla-1.2
        if [ ! -f /opt/scylladb/lib/scylla/scylla_config_get.py ]; then
            echo
            echo "Error: Upgrading from scylla-$ver to scylla-%{version} is not supported."
            echo "Please upgrade to scylla-1.7.3 or later, before upgrade to %{version}."
            echo
            exit 1
        fi
        commitlog_directory=$(/opt/scylladb/lib/scylla/scylla_config_get.py -g commitlog_directory)
        commitlog_files=$(ls $commitlog_directory | wc -l)
        if [ $commitlog_files -ne 0 ]; then
            echo
            echo "Error: Upgrading from scylla-$ver to scylla-%{version} is not supported when commitlog is not clean."
            echo "Please upgrade to scylla-1.7.3 or later, before upgrade to %{version}."
            echo "Also make sure $commitlog_directory is empty."
            echo
            exit 1
        fi
    fi
fi

%files conf
%defattr(-,root,root)
%attr(0755,root,root) %dir %{_sysconfdir}/scylla
%config(noreplace) %{_sysconfdir}/scylla/scylla.yaml
%config(noreplace) %{_sysconfdir}/scylla/cassandra-rackdc.properties
%if 0%{housekeeping}
%config(noreplace) %{_sysconfdir}/scylla.d/housekeeping.cfg
%endif


%package kernel-conf
Group:          Applications/Databases
Summary:        Scylla configuration package for the Linux kernel
License:        AGPLv3
URL:            http://www.scylladb.com/
Requires:       kmod
# tuned overwrites our sysctl settings
Obsoletes:      tuned >= 2.11.0

%description kernel-conf
This package contains Linux kernel configuration changes for the Scylla database.  Install this package
if Scylla is the main application on your server and you wish to optimize its latency and throughput.

%post kernel-conf
# We cannot use the sysctl_apply rpm macro because it is not present in 7.0
# following is a "manual" expansion
/usr/lib/systemd/systemd-sysctl 99-scylla-sched.conf >/dev/null 2>&1 || :
/usr/lib/systemd/systemd-sysctl 99-scylla-vm.conf >/dev/null 2>&1 || :
/usr/lib/systemd/systemd-sysctl 99-scylla-inotify.conf >/dev/null 2>&1 || :
/usr/lib/systemd/systemd-sysctl 99-scylla-aio.conf >/dev/null 2>&1 || :
/usr/lib/systemd/systemd-sysctl 99-scylla-filemax.conf >/dev/null 2>&1 || :

%files kernel-conf
%defattr(-,root,root)
%{_sysctldir}/*.conf


%package node-exporter
Group:          Applications/Databases
Summary:        Prometheus exporter for machine metrics
License:        ASL 2.0
URL:            https://github.com/prometheus/node_exporter

%description node-exporter
Prometheus exporter for machine metrics, written in Go with pluggable metric collectors.

%post node-exporter
if [ $1 -eq 1 ] ; then
    /usr/bin/systemctl preset scylla-node-exporter.service ||:
fi

%preun node-exporter
if [ $1 -eq 0 ] ; then
    /usr/bin/systemctl --no-reload disable scylla-node-exporter.service ||:
    /usr/bin/systemctl stop scylla-node-exporter.service ||:
fi

%postun node-exporter
/usr/bin/systemctl daemon-reload ||:

%files node-exporter
%defattr(-,root,root)
%config(noreplace) %{_sysconfdir}/sysconfig/scylla-node-exporter
%{_unitdir}/scylla-node-exporter.service
/opt/scylladb/node_exporter/node_exporter
/opt/scylladb/node_exporter/licenses/LICENSE
/opt/scylladb/node_exporter/licenses/NOTICE
/etc/systemd/system/scylla-node-exporter.service.d/dependencies.conf

%changelog
* Tue Jul 21 2015 Takuya ASADA <syuu@cloudius-systems.com>
- inital version of scylla.spec
