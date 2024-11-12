import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  mainSidebar: [
    'intro',
    'download',
    {
      type: 'category',
      label: 'Configuration',
      link: {
        type: 'generated-index',
      },
      items: [
        {
          type: 'doc',
          id: 'config/overview',
          label: 'Overview',
        },
        'config/route-matchers',
        'config/templating',
        'config/environment-variables',
      ],
    },
    {
      type: 'category',
      label: 'Sending Data',
      link: {
        type: 'generated-index',
      },
      items: [
        {
          type: 'doc',
          id: 'sending-data/overview',
          label: 'Overview',
        },
        'sending-data/openapi',
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      link: {
        type: 'generated-index',
      },
      items: [
        'operations/metrics',
      ],
    },
  ],
};

export default sidebars;
